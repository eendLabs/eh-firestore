package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/eendLabs/eh-firestore/pkg/share"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"google.golang.org/api/iterator"
)

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

// ErrCouldNotMarshalEvent is when an event could not be marshaled into BSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshalled
//into a concrete type.
var ErrCouldNotUnmarshalEvent = errors.New("could not unmarshal event")

// ErrCouldNotSaveAggregate is when an aggregate could not be saved.
var ErrCouldNotSaveAggregate = errors.New("could not save aggregate")

// EventStore implements an EventStore for DynamoDB.
type EventStore struct {
	client  *firestore.Client
	config  *share.Config
	encoder share.Encoder
}

// NewEventStore creates a new EventStore.
func NewEventStore(
	config *share.Config) (*EventStore, error) {
	config.ProvideDefaults()

	client, err := firestore.NewClient(context.TODO(), config.ProjectID)
	if err != nil {
		return nil, errors.New(fmt.Sprintf(share.ErrCouldNotDialDB, err))
	}

	return NewEventStoreWithClient(config, client)
}

// NewEventStoreWithClient creates a new EventStore with DB
func NewEventStoreWithClient(config *share.Config,
	client *firestore.Client) (*EventStore, error) {
	if client == nil {
		return nil, share.ErrNoDBClient
	}

	s := &EventStore{
		client:  client,
		config:  config,
		encoder: &share.JsonEncoder{},
	}

	s.config.DbName = func(ctx context.Context) string {
		ns := eh.NamespaceFromContext(ctx)
		return s.config.Collection + "_" + ns
	}

	return s, nil
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event,
	originalVersion int) error {
	ns := eh.NamespaceFromContext(ctx)

	if len(events) == 0 {
		return eh.EventStoreError{
			Err:       eh.ErrNoEventsToAppend,
			Namespace: ns,
		}
	}

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	dbEvents := make([]aggregateEvent, len(events))
	aggregateID := events[0].AggregateID()
	aggregateType := events[0].AggregateType()
	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != aggregateID {
			return eh.EventStoreError{
				Err:       eh.ErrInvalidEvent,
				Namespace: ns,
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != originalVersion+i+1 {
			return eh.EventStoreError{
				Err:       eh.ErrIncorrectEventVersion,
				Namespace: ns,
			}
		}

		// Create the event record for the DB.
		e, err := s.newAggregateEvent(ctx, event)
		if err != nil {
			return err
		}
		dbEvents[i] = *e

	}

	// Either insert a new aggregate or append to an existing.
	err := s.client.RunTransaction(ctx, func(ctx context.Context,
		tx *firestore.Transaction) error {
		aggregate := &aggregateRecord{
			AggregateID:   aggregateID.String(),
			AggregateType: aggregateType,
		}

		if originalVersion == 0 {
			aggregate.Version = len(dbEvents)
			if err := tx.Set(s.client.Collection(s.config.DbName(ctx)).
				Doc(aggregateID.String()), aggregate); err != nil {
				return eh.EventStoreError{
					Err:       ErrCouldNotSaveAggregate,
					BaseErr:   err,
					Namespace: ns,
				}
			}
		} else {
			query := s.client.Collection(s.config.DbName(ctx)).
				Where("aggregateID", "==", aggregateID.String()).
				Where("version", "==", originalVersion)
			iter := query.Documents(ctx)
			aggregates, _ := iter.GetAll()
			if err := tx.Update(aggregates[0].Ref, []firestore.Update{
				{
					Path:  "version",
					Value: firestore.Increment(len(dbEvents)),
				},
			}); err != nil {
				return eh.EventStoreError{
					Err:       ErrCouldNotSaveAggregate,
					BaseErr:   err,
					Namespace: ns,
				}
			}
		}

		eventCollection := s.client.Collection(s.config.DbName(ctx)).Doc(
			aggregateID.String()).Collection("events")

		for _, e := range dbEvents {
			item := eventCollection.Doc(e.EventID)

			if err := tx.Set(item, e); err != nil {
				return eh.EventStoreError{
					Err:       ErrCouldNotSaveAggregate,
					BaseErr:   err,
					Namespace: ns,
				}
			}
		}

		return nil
	})

	if err != nil {
		return eh.EventStoreError{
			BaseErr:   err,
			Err:       ErrCouldNotSaveAggregate,
			Namespace: ns,
		}
	}

	return nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (s *EventStore) Load(ctx context.Context,
	u uuid.UUID) ([]eh.Event, error) {
	ns := eh.NamespaceFromContext(ctx)
	query := s.client.
		Collection(s.config.DbName(ctx)).
		Doc(u.String()).
		Collection("events").
		OrderBy("version", firestore.Asc)
	iter := query.Documents(ctx)
	aggregateEvents, _ := iter.GetAll()

	events := make([]eh.Event, len(aggregateEvents))
	for i, e := range aggregateEvents {
		var aggEvent aggregateEvent
		if err := e.DataTo(&aggEvent); err != nil {
			return nil, eh.EventStoreError{
				Err:       ErrCouldNotUnmarshalEvent,
				BaseErr:   err,
				Namespace: ns,
			}
		}
		// Create an event of the correct type and decode from raw BSON.
		if len(aggEvent.RawData) > 0 {
			var err error
			if aggEvent.data, err = eh.CreateEventData(aggEvent.EventType); err != nil {
				return nil, eh.EventStoreError{
					Err:       ErrCouldNotUnmarshalEvent,
					BaseErr:   err,
					Namespace: ns,
				}
			}
			if aggEvent.data, err = s.encoder.
				Unmarshal(aggEvent.EventType, []byte(aggEvent.RawData)); err != nil {
				return nil, eh.EventStoreError{
					Err:       ErrCouldNotUnmarshalEvent,
					BaseErr:   err,
					Namespace: ns,
				}
			}
			aggEvent.RawData = ""
		}
		event := eh.NewEvent(
			aggEvent.EventType,
			aggEvent.data,
			aggEvent.Timestamp,
			eh.ForAggregate(
				aggEvent.AggregateType,
				uuid.MustParse(aggEvent.AggregateID),
				aggEvent.Version,
			),
			eh.WithMetadata(aggEvent.Metadata),
		)
		events[i] = event
	}
	return events, nil
}

// Replace implements the Replace method of the eventhorizon.EventStore
//interface.
func (s *EventStore) Replace(ctx context.Context, event eh.Event) error {
	ns := eh.NamespaceFromContext(ctx)
	err := s.client.RunTransaction(ctx, func(ctx context.Context,
		tx *firestore.Transaction) error {

		aggRecord, err := s.client.Collection(s.config.DbName(ctx)).
			Doc(event.AggregateID().String()).
			Get(ctx)
		if aggRecord == nil {
			return eh.ErrAggregateNotFound
		}
		if aggRecord.Exists() == false {
			return eh.ErrAggregateNotFound
		}
		if err != nil {
			return eh.EventStoreError{
				Err:       err,
				Namespace: ns,
			}
		}

		query := aggRecord.Ref.
			Collection("events").
			Where("version", "==", event.Version())
		aggEventIter := query.Documents(ctx)
		aggEvents, err := aggEventIter.GetAll()
		if err != nil {
			return eh.EventStoreError{
				Err:       err,
				Namespace: ns,
			}
		}

		if len(aggEvents) == 0 {
			return eh.ErrInvalidEvent
		}

		for _, aggEvent := range aggEvents {
			// Create the event record for the Database.
			e, err := s.newAggregateEvent(ctx, event)
			if err != nil {
				return err
			}

			err = tx.Set(aggEvent.Ref, e)
			if err != nil {
				return eh.EventStoreError{
					Err:       err,
					Namespace: ns,
				}
			}

		}

		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

// RenameEvent implements the RenameEvent method of the eventhorizon.EventStore
//interface.
func (s *EventStore) RenameEvent(ctx context.Context,
	from, to eh.EventType) error {
	ns := eh.NamespaceFromContext(ctx)

	err := s.client.RunTransaction(ctx, func(ctx context.Context,
		tx *firestore.Transaction) error {
		query := s.client.Collection(s.config.DbName(ctx))
		iter := query.Documents(ctx)
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			query := doc.Ref.Collection("events").
				Where("eventType", "==", from.String()).
				OrderBy("version", firestore.Asc)
			iter := query.Documents(ctx)

			for {
				doc, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				if err := tx.Update(doc.Ref, []firestore.Update{
					{
						Path:  "eventType",
						Value: to.String(),
					},
				}); err != nil {
					return eh.EventStoreError{
						Err:       ErrCouldNotSaveAggregate,
						BaseErr:   err,
						Namespace: ns,
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return eh.EventStoreError{
			Err:       ErrCouldNotSaveAggregate,
			BaseErr:   err,
			Namespace: ns,
		}
	}
	return nil
}

// Close closes the database client.
func (s *EventStore) Close(_ context.Context) {
	if err := s.client.Close(); err != nil {
		log.Fatalf("could not close the client: %v", err)
	}
}

// Clear clears the event storage.
func (s *EventStore) Clear(ctx context.Context) error {
	ns := eh.NamespaceFromContext(ctx)

	err := s.client.RunTransaction(ctx, func(ctx context.Context,
		tx *firestore.Transaction) (err error) {
		query := s.client.Collection(s.config.DbName(ctx))
		iter := query.Documents(ctx)
		for {
			doc, err := iter.Next()

			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			err = tx.Delete(doc.Ref)
		}

		return err
	})
	if err != nil {
		return eh.EventStoreError{
			BaseErr:   err,
			Err:       ErrCouldNotClearDB,
			Namespace: ns,
		}
	}
	return nil
}

// aggregateRecord is the Database representation of an aggregate.
type aggregateRecord struct {
	AggregateID   string           `firestore:"aggregateID"`
	AggregateType eh.AggregateType `firestore:"aggregateType"`
	Version       int              `firestore:"version"`
}

// aggregateEvent is the internal event record for the MongoDB event store used
// to save and load events from the DB.
type aggregateEvent struct {
	AggregateID   string                 `firestore:"aggregateID"`
	AggregateType eh.AggregateType       `firestore:"aggregateType"`
	data          eh.EventData           `firestore:"-"`
	EventID       string                 `firestore:"eventID"`
	EventType     eh.EventType           `firestore:"eventType"`
	Metadata      map[string]interface{} `firestore:"metadata"`
	RawData       string                 `firestore:"data,omitempty"`
	Timestamp     time.Time              `firestore:"timestamp"`
	Version       int                    `firestore:"version"`
}

// newAggregateEvent returns a new evt for an event.
func (s *EventStore) newAggregateEvent(ctx context.Context,
	event eh.Event) (*aggregateEvent, error) {
	ns := eh.NamespaceFromContext(ctx)

	// Marshal event data if there is any.
	raw, err := s.encoder.Marshal(event.Data())
	if err != nil {
		return nil, eh.EventStoreError{
			BaseErr:   err,
			Err:       ErrCouldNotMarshalEvent,
			Namespace: ns,
		}
	}

	return &aggregateEvent{
		AggregateType: event.AggregateType(),
		AggregateID:   event.AggregateID().String(),
		EventID:       uuid.New().String(),
		EventType:     event.EventType(),
		Metadata:      event.Metadata(),
		RawData:       string(raw),
		Timestamp:     event.Timestamp(),
		Version:       event.Version(),
	}, nil
}

// event is the private implementation of the eventhorizon.Event interface
// for a MongoDB event store.
type event struct {
	aggregateEvent
}

// AggrgateID implements the AggrgateID method of the eventhorizon.Event
//interface.
func (e event) AggregateID() uuid.UUID {
	return uuid.MustParse(e.aggregateEvent.AggregateID)
}

// AggregateType implements the AggregateType method of the eventhorizon.Event
//interface.
func (e event) AggregateType() eh.AggregateType {
	return e.aggregateEvent.AggregateType
}

// EventType implements the EventType method of the eventhorizon.Event
//interface.
func (e event) EventType() eh.EventType {
	return e.aggregateEvent.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.aggregateEvent.data
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.aggregateEvent.Version
}

// Timestamp implements the Timestamp method of the eventhorizon.Event
//interface.
func (e event) Timestamp() time.Time {
	return e.aggregateEvent.Timestamp
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.aggregateEvent.EventType,
		e.aggregateEvent.Version)
}

func (e event) Metadata() map[string]interface{} {
	return e.aggregateEvent.Metadata
}

package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"log"
	"time"
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

// EventStoreConfig is a config for the Firestore event store.
type EventStoreConfig struct {
	ProjectID  string
	Collection string
}

func (c *EventStoreConfig) provideDefaults() {
	if c.ProjectID == "" {
		c.ProjectID = "eventhorizonEvents"
	}
	if c.Collection == "" {
		c.Collection = "us-east-1"
	}
}

// EventStore implements an EventStore for DynamoDB.
type EventStore struct {
	service *firestore.Client
	config  *EventStoreConfig
}

// NewEventStore creates a new EventStore.
func NewEventStore(ctx context.Context,
	config *EventStoreConfig) (*EventStore, error) {
	config.provideDefaults()

	client, err := firestore.NewClient(ctx, config.ProjectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
		//return nil, err
	}

	return NewEventStoreWithClient(config, client), nil
}

// NewEventStoreWithDB creates a new EventStore with DB
func NewEventStoreWithClient(config *EventStoreConfig,
	client *firestore.Client) *EventStore {
	s := &EventStore{
		service: client,
		config:  config,
	}

	return s
}

// dbEvent is the internal event record for the Firestore event store used
// to save and load events from the DB.
type dbEvent struct {
	AggregateID uuid.UUID
	Version     int

	EventType     eh.EventType
	RawData       map[string]interface{}
	data          eh.EventData
	Timestamp     time.Time
	AggregateType eh.AggregateType
}

package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type AggregateRecord struct {
	Namespace     string
	AggregateID   uuid.UUID
	AggregateType eh.AggregateType
	Version       int
}

type AggregateEvent struct {
	EventID       uuid.UUID
	Namespace     string
	AggregateID   uuid.UUID
	AggregateType eh.AggregateType
	EventType     eh.EventType
	RawData       json.RawMessage
	Timestamp     time.Time
	Version       int
	Context       map[string]interface{}
	data          eh.EventData
}

// event is the private implementation of the eventhorizon.Event interface
// for a MongoDB event store.
type event struct {
	AggregateEvent
}

// AggregateID implements the AggregateID method of the eventhorizon.Event interface.
func (e event) AggregateID() uuid.UUID {
	return e.AggregateEvent.AggregateID
}

// AggregateType implements the AggregateType method of the eventhorizon.Event interface.
func (e event) AggregateType() eh.AggregateType {
	return e.AggregateEvent.AggregateType
}

// EventType implements the EventType method of the eventhorizon.Event interface.
func (e event) EventType() eh.EventType {
	return e.AggregateEvent.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.AggregateEvent.data
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.AggregateEvent.Version
}

// Timestamp implements the Timestamp method of the eventhorizon.Event interface.
func (e event) Timestamp() time.Time {
	return e.AggregateEvent.Timestamp
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.AggregateEvent.EventType,
		e.AggregateEvent.Version)
}

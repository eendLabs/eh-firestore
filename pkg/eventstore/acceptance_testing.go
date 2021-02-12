package eventstore

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/mocks"
	"strings"
	"testing"
	"time"
)

// AcceptanceTest is the acceptance test that all implementations of EventStore
// should pass. It should manually be called from a test case in each
// implementation:
//
//   func TestEventStore(t *testing.T) {
//       ctx := context.Background() // Or other when testing namespaces.
//       store := NewEventStore()
//       eventstore.AcceptanceTest(t, ctx, store)
//   }
// Note:
//
// This is a exact copy from the source code https://github.com/looplab/eventhorizon/blob/0be614d4bc188f244dea9a390633905abf671906/eventstore/acceptanece_testing.go
// the main reason of this is firestore doesn't store int32, they converted to
// int64 and the test was failing when checking the metadata payload
func AcceptanceTest(t *testing.T, ctx context.Context,
	store eh.EventStore) []eh.Event {
	savedEvents := []eh.Event{}

	ctx = context.WithValue(ctx, "testkey", "testval")

	// Save no events.
	err := store.Save(ctx, []eh.Event{}, 0)
	if esErr, ok := err.(eh.EventStoreError); !ok ||
		esErr.Err != eh.ErrNoEventsToAppend {
		t.Error("there shoud be a ErrNoEventsToAppend error:", err)
	}

	// Save event, version 1.
	id := uuid.New()
	timestamp := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	event1 := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "event1"},
		timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 1))
	err = store.Save(ctx, []eh.Event{event1}, 0)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	savedEvents = append(savedEvents, event1)
	// if val, ok := agg.Context.Value("testkey").(string);
	//!ok || val != "testval" {
	// 	t.Error("the context should be correct:", agg.Context)
	// }

	// Try to save same event twice.
	err = store.Save(ctx, []eh.Event{event1}, 1)
	if esErr, ok := err.(eh.EventStoreError); !ok ||
		esErr.Err != eh.ErrIncorrectEventVersion {
		t.Error("there should be a ErrIncerrectEventVersion error:",
			err)
	}

	// Save event, version 2, with metadata.
	event2 := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "event2"},
		timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 2),
		eh.WithMetadata(map[string]interface{}{"meta": "data",
			"num": int64(42)}),
	)
	err = store.Save(ctx, []eh.Event{event2}, 1)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	savedEvents = append(savedEvents, event2)

	// Save event without data, version 3.
	event3 := eh.NewEvent(mocks.EventOtherType, nil, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 3))
	err = store.Save(ctx, []eh.Event{event3}, 2)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	savedEvents = append(savedEvents, event3)

	// Save multiple events, version 4,5 and 6.
	event4 := eh.NewEvent(mocks.EventOtherType, nil, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 4))
	event5 := eh.NewEvent(mocks.EventOtherType, nil, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 5))
	event6 := eh.NewEvent(mocks.EventOtherType, nil, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 6))
	err = store.Save(ctx, []eh.Event{event4, event5, event6}, 3)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	savedEvents = append(savedEvents, event4, event5, event6)

	// Save event for another aggregate.
	id2 := uuid.New()
	event7 := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "event7"},
		timestamp,
		eh.ForAggregate(mocks.AggregateType, id2, 1))
	err = store.Save(ctx, []eh.Event{event7}, 0)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	savedEvents = append(savedEvents, event7)

	// Load events for non-existing aggregate.
	events, err := store.Load(ctx, uuid.New())
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if len(events) != 0 {
		t.Error("there should be no loaded events:",
			eventsToString(events))
	}

	// Load events.
	events, err = store.Load(ctx, id)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	expectedEvents := []eh.Event{
		event1,                 // Version 1
		event2,                 // Version 2
		event3,                 // Version 3
		event4, event5, event6, // Version 4, 5 and 6
	}
	for i, event := range events {
		if err := mocks.CompareEvents(event, expectedEvents[i]); err != nil {
			t.Error("the event was incorrect:", err)
		}
		if event.Version() != i+1 {
			t.Error("the event version should be correct:", event,
				event.Version())
		}
	}

	// Load events for another aggregate.
	events, err = store.Load(ctx, id2)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	expectedEvents = []eh.Event{event7}
	for i, event := range events {
		if err := mocks.CompareEvents(event, expectedEvents[i]); err != nil {
			t.Error("the event was incorrect:", err)
		}
		if event.Version() != i+1 {
			t.Error("the event version should be correct:", event,
				event.Version())
		}
	}

	return savedEvents
}

func eventsToString(events []eh.Event) string {
	parts := make([]string, len(events))
	for i, e := range events {
		parts[i] = fmt.Sprintf("%s:%s (%s)",
			e.AggregateType(), e.EventType(), e.AggregateID())
	}
	return strings.Join(parts, ", ")
}

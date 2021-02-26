package eventstore

import (
	"context"
	"testing"

	"github.com/eendLabs/eh-firestore/pkg/share"
	eh "github.com/looplab/eventhorizon"
	testutil "github.com/looplab/eventhorizon/eventstore"
)

func TestEventStore(t *testing.T) {
	ctx := context.Background()
	store, err := NewEventStore(&share.Config{
		ProjectID:  "dummy-project-id",
		Collection: "test",
	})

	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}

	ctx = eh.NewContextWithNamespace(context.Background(), "ns")
	defer func() {
		t.Log("clearing db")
		if err = store.Clear(context.Background()); err != nil {
			t.Fatal("there should be no error:", err)
		}
		if err = store.Clear(ctx); err != nil {
			t.Fatal("there should be no error:", err)
		}
	}()

	// Run the actual test suite.
	t.Log("event store with default namespace")
	AcceptanceTest(t, context.Background(), store)

	t.Log("event store with other namespace")
	AcceptanceTest(t, context.Background(), store)

	t.Log("event store maintainer")
	testutil.MaintainerAcceptanceTest(t, context.Background(), store)
}

package share

import (
	"context"
)

// EventStoreConfig is a config for the Firestore event store.
type Config struct {
	Collection string
	ProjectID  string
	DbName     func(ctx context.Context) string
}

func (c *Config) ProvideDefaults() {
	if c.ProjectID == "" {
		c.ProjectID = "eventhorizonEvents"
	}
	if c.Collection == "" {
		c.Collection = "us-east-1"
	}
}

package share

import "errors"

// ErrCouldNotDialDB is when the database could not be dialed.
var ErrCouldNotDialDB = "could not dial database %v"

// ErrNoDBClient is when no database client is set.
var ErrNoDBClient = errors.New("no database client")

package contracts

import dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"

// Runtime is the narrow consumer-facing DBOS integration contract. Application
// services normally depend on this interface or on an even narrower local
// interface instead of the concrete Goravel service provider implementation.
type Runtime interface {
	Client() (dbossdk.Client, error)
	Role() string
	ApplicationVersion() string
	DatabaseConnection() string
	DatabaseDriver() string
	DatabaseSchema() string
}

// ApplicationRuntime identifies the application that owns newly enqueued
// workflows. PostgreSQL transaction-bound enqueue requires this additional
// contract. Runtime remains unchanged for existing consumer interfaces.
type ApplicationRuntime interface {
	Runtime
	ApplicationName() string
}

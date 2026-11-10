package goraveldbos

import (
	"fmt"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"

	packagecontracts "github.com/freshost/goravel-dbos/contracts"
)

// Enqueue submits a typed workflow through the runtime's producer Client.
func Enqueue[Input any, Output any](
	runtime packagecontracts.Runtime,
	definition Workflow[Input, Output],
	input Input,
	options ...dbossdk.EnqueueOption,
) (dbossdk.WorkflowHandle[Output], error) {
	if runtime == nil {
		return nil, fmt.Errorf("goravel-dbos: runtime is required")
	}
	if definition.name == "" || definition.queue == "" {
		return nil, fmt.Errorf("goravel-dbos: invalid zero-value workflow definition")
	}
	client, err := runtime.Client()
	if err != nil {
		return nil, err
	}
	// A standalone DBOS Client has no application-version field in ClientConfig.
	// Force the package's validated version so separate producer and worker
	// binaries always target the same workflow code.
	options = append(options, dbossdk.WithEnqueueApplicationVersion(runtime.ApplicationVersion()))
	if application, ok := runtime.(packagecontracts.ApplicationRuntime); ok {
		options = append(options, dbossdk.WithEnqueueApplicationName(application.ApplicationName()))
	}
	handle, err := dbossdk.Enqueue[Output](client, definition.queue, definition.name, input, options...)
	if err != nil {
		return nil, fmt.Errorf("goravel-dbos: enqueue workflow %q: %w", definition.name, err)
	}
	return handle, nil
}

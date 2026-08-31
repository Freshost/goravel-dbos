package goraveldbos

import (
	"fmt"
	"strings"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Workflow is a typed, stable producer/worker contract. Keep Name versioned
// when changing a serialized input incompatibly.
type Workflow[Input any, Output any] struct {
	name  string
	queue string
}

// Define declares a typed workflow contract. It panics on invalid constants so
// bad package-level declarations fail during application startup.
func Define[Input any, Output any](name, queue string) Workflow[Input, Output] {
	name = strings.TrimSpace(name)
	queue = strings.TrimSpace(queue)
	if name == "" || queue == "" {
		panic("goravel-dbos: workflow name and queue are required")
	}
	return Workflow[Input, Output]{name: name, queue: queue}
}

func (w Workflow[Input, Output]) Name() string  { return w.name }
func (w Workflow[Input, Output]) Queue() string { return w.queue }

// RegisterWorkflow binds a typed definition to its application implementation.
// The definition's stable name always wins over any caller-supplied name option.
func RegisterWorkflow[Input any, Output any](
	registry *Registry,
	definition Workflow[Input, Output],
	workflow dbossdk.Workflow[Input, Output],
	options ...dbossdk.WorkflowRegistrationOption,
) error {
	if registry == nil || registry.ctx == nil {
		return fmt.Errorf("goravel-dbos: registry is unavailable")
	}
	if workflow == nil {
		return fmt.Errorf("goravel-dbos: workflow %q implementation is nil", definition.name)
	}
	if err := registry.markWorkflow(definition.name); err != nil {
		return err
	}
	options = append(options, dbossdk.WithWorkflowName(definition.name))
	dbossdk.RegisterWorkflow(registry.ctx, workflow, options...)
	return nil
}

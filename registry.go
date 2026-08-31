package goraveldbos

import (
	"errors"
	"fmt"
	"sync"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Registrar registers application-owned queues, workflows and schedules on an
// executor context before DBOS is launched.
type Registrar interface {
	RegisterDBOS(registry *Registry) error
}

// RegistrarFunc adapts a function to Registrar.
type RegistrarFunc func(registry *Registry) error

func (fn RegistrarFunc) RegisterDBOS(registry *Registry) error { return fn(registry) }

// AfterLauncher is an optional registrar hook for application-specific startup
// reconciliation that must run only after DBOS successfully launches.
type AfterLauncher interface {
	AfterDBOSLaunch() error
}

// Registry is handed to application registrars. It exposes the underlying DBOS
// Context for advanced SDK features while centralizing queue and schedule error
// handling and detecting duplicate package workflow descriptors.
type Registry struct {
	ctx       dbossdk.Context
	mu        sync.Mutex
	workflows map[string]struct{}
	queues    map[string]dbossdk.Queue
	schedules map[string]dbossdk.ScheduleSpec
}

func newRegistry(ctx dbossdk.Context) *Registry {
	return &Registry{
		ctx:       ctx,
		workflows: make(map[string]struct{}),
		queues:    make(map[string]dbossdk.Queue),
		schedules: make(map[string]dbossdk.ScheduleSpec),
	}
}

// Context returns the unlaunched DBOS executor context. Workflow registration
// must finish before Runtime.Start launches it.
func (r *Registry) Context() dbossdk.Context { return r.ctx }

// Queue registers or reconciles a database-backed queue.
func (r *Registry) Queue(name string, options ...dbossdk.QueueOption) (dbossdk.Queue, error) {
	if r == nil || r.ctx == nil {
		return nil, errors.New("goravel-dbos: registry context is unavailable")
	}
	if name == "" {
		return nil, errors.New("goravel-dbos: queue name is required")
	}
	queue, err := dbossdk.RegisterQueue(r.ctx, name, options...)
	if err != nil {
		return nil, fmt.Errorf("goravel-dbos: register queue %q: %w", name, err)
	}
	r.mu.Lock()
	r.queues[name] = queue
	r.mu.Unlock()
	return queue, nil
}

// QueueByName returns a queue already registered through this Registry.
func (r *Registry) QueueByName(name string) (dbossdk.Queue, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	queue, exists := r.queues[name]
	return queue, exists
}

// Schedule declares a database-backed DBOS schedule. Runtime.Start applies all
// declared schedules as one restart-safe upsert batch before launching DBOS.
func (r *Registry) Schedule(spec dbossdk.ScheduleSpec) error {
	if r == nil || r.ctx == nil {
		return errors.New("goravel-dbos: registry context is unavailable")
	}
	if spec.ScheduleName == "" {
		return errors.New("goravel-dbos: schedule name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.schedules[spec.ScheduleName]; exists {
		return fmt.Errorf("goravel-dbos: schedule %q registered more than once", spec.ScheduleName)
	}
	r.schedules[spec.ScheduleName] = spec
	return nil
}

func (r *Registry) applySchedules() error {
	r.mu.Lock()
	schedules := make([]dbossdk.ScheduleSpec, 0, len(r.schedules))
	for _, spec := range r.schedules {
		schedules = append(schedules, spec)
	}
	r.mu.Unlock()
	if len(schedules) == 0 {
		return nil
	}
	if err := dbossdk.ApplySchedules(r.ctx, schedules); err != nil {
		return fmt.Errorf("goravel-dbos: apply schedules: %w", err)
	}
	return nil
}

func (r *Registry) markWorkflow(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.workflows[name]; exists {
		return fmt.Errorf("goravel-dbos: workflow descriptor %q registered more than once", name)
	}
	r.workflows[name] = struct{}{}
	return nil
}

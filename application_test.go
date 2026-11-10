package goraveldbos

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/goravel/framework/contracts/binding"
	contractsconfig "github.com/goravel/framework/contracts/config"
	contractsfoundation "github.com/goravel/framework/contracts/foundation"
	frameworkfoundation "github.com/goravel/framework/foundation"
	mocksconfig "github.com/goravel/framework/mocks/config"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type applicationConfig struct {
	values map[string]any
}

func newApplicationConfig(values map[string]any) *applicationConfig {
	return &applicationConfig{values: values}
}

func (c *applicationConfig) Env(name string, defaults ...any) any {
	return c.Get(name, defaults...)
}

func (c *applicationConfig) EnvString(name string, defaults ...string) string {
	return c.GetString(name, defaults...)
}

func (c *applicationConfig) EnvBool(name string, defaults ...bool) bool {
	return c.GetBool(name, defaults...)
}

func (c *applicationConfig) Add(name string, value any) { c.values[name] = value }

func (c *applicationConfig) Get(name string, defaults ...any) any {
	if value, ok := c.values[name]; ok {
		return value
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return nil
}

func (c *applicationConfig) GetString(name string, defaults ...string) string {
	if value, ok := c.values[name]; ok {
		return fmt.Sprint(value)
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return ""
}

func (c *applicationConfig) GetInt(name string, defaults ...int) int {
	if value, ok := c.values[name].(int); ok {
		return value
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return 0
}

func (c *applicationConfig) GetBool(name string, defaults ...bool) bool {
	if value, ok := c.values[name].(bool); ok {
		return value
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return false
}

func (c *applicationConfig) GetDuration(name string, defaults ...time.Duration) time.Duration {
	if value, ok := c.values[name].(time.Duration); ok {
		return value
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return 0
}

func (c *applicationConfig) GetStringSlice(name string, defaults ...[]string) []string {
	if value, ok := c.values[name].([]string); ok {
		return value
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return nil
}

func (c *applicationConfig) UnmarshalKey(string, any) error { return nil }

var _ contractsconfig.Config = (*applicationConfig)(nil)

func TestServiceProviderUsesRealGoravelContainer(t *testing.T) {
	previousApp := App
	t.Cleanup(func() { App = previousApp })

	app := frameworkfoundation.NewApplication()
	config := newApplicationConfig(map[string]any{
		"dbos.enabled": false,
		"dbos.role":    RoleProducer,
	})
	app.Instance(binding.Config, config)

	provider := &ServiceProvider{}
	provider.Register(app)
	provider.Boot(app)

	runtime, err := Resolve(app)
	require.NoError(t, err)
	require.Equal(t, RoleDisabled, runtime.Role())
	require.Same(t, app, App)
	require.Len(t, provider.Runners(app), 1)
	require.Equal(t, []string{Binding}, provider.Relationship().Bindings)
}

func TestServiceProviderDisablesUnneededWorkerRunners(t *testing.T) {
	app := mocksfoundation.NewApplication(t)
	config := mocksconfig.NewConfig(t)
	app.EXPECT().Commands(mock.Anything).Once()
	app.EXPECT().ConfigPath("dbos.go").Return("/app/config/dbos.go").Once()
	app.EXPECT().Publishes(PackageName, map[string]string{
		"setup/config/dbos.go": "/app/config/dbos.go",
	}, "goravel-dbos").Once()
	app.EXPECT().MakeConfig().Return(config).Once()
	config.EXPECT().GetBool("dbos.enabled", true).Return(true).Once()
	config.EXPECT().GetString("dbos.role", RoleProducer).Return(RoleWorker).Once()
	config.EXPECT().GetBool("dbos.worker.disable_goravel_runners", true).Return(true).Once()
	config.EXPECT().GetStringSlice("app.disabled_runners", []string{}).Return([]string{"application:metrics", "goravel:http"}).Once()
	config.EXPECT().GetStringSlice("dbos.worker.disabled_runners", defaultWorkerDisabledRunners).Return([]string{"goravel:http", "goravel:queue"}).Once()
	config.EXPECT().Add("app.disabled_runners", []string{"application:metrics", "goravel:http", "goravel:queue"}).Once()

	(&ServiceProvider{}).Boot(app)
}

func TestDisabledRuntimeUsesNativeGoravelConfig(t *testing.T) {
	app := mocksfoundation.NewApplication(t)
	app.EXPECT().MakeConfig().Return(newApplicationConfig(map[string]any{
		"dbos.enabled": false,
		"dbos.role":    RoleProducer,
	})).Once()

	runtime, err := NewRuntime(app)
	require.NoError(t, err)
	require.Equal(t, RoleDisabled, runtime.Role())
}

func TestRuntimeDisabledOperations(t *testing.T) {
	runtime := &Runtime{config: Config{Role: RoleDisabled, ShutdownTimeout: time.Second}}
	require.Error(t, runtime.AddRegistrar(nil))
	require.NoError(t, runtime.AddRegistrar(RegistrarFunc(func(*Registry) error { return nil })))
	require.ErrorIs(t, runtime.Start(), ErrDisabled)
	_, err := runtime.Client()
	require.ErrorIs(t, err, ErrDisabled)
	_, err = runtime.Executor()
	require.ErrorIs(t, err, ErrNotExecutor)
	require.NoError(t, runtime.Shutdown())
	require.NoError(t, runtime.Shutdown())
	require.Error(t, runtime.AddRegistrar(RegistrarFunc(func(*Registry) error { return nil })))
}

func TestRuntimeBuildsOwnedPostgresPoolFromNativeConnection(t *testing.T) {
	app := mocksfoundation.NewApplication(t)
	app.EXPECT().Context().Return(context.Background()).Once()
	runtime := &Runtime{app: app, config: Config{
		AppName:             "billing",
		DatabaseDriver:      "postgres",
		DatabaseURL:         "postgresql://app:secret@db.internal:5432/billing?sslmode=require",
		PoolMaxConns:        10,
		PoolMinConns:        5,
		PoolMaxConnLifetime: 2 * time.Hour,
		PoolMaxConnIdleTime: 2 * time.Minute,
		PoolConnectTimeout:  7 * time.Second,
	}}

	databaseURL, pool, err := runtime.databaseInputs()
	require.NoError(t, err)
	require.Empty(t, databaseURL)
	require.NotNil(t, pool)
	t.Cleanup(pool.Close)
	poolConfig := pool.Config()
	require.Equal(t, int32(10), poolConfig.MaxConns)
	require.Equal(t, int32(5), poolConfig.MinConns)
	require.Equal(t, 2*time.Hour, poolConfig.MaxConnLifetime)
	require.Equal(t, 2*time.Minute, poolConfig.MaxConnIdleTime)
	require.Equal(t, 7*time.Second, poolConfig.ConnConfig.ConnectTimeout)
	require.Equal(t, "billing", poolConfig.ConnConfig.RuntimeParams["application_name"])
}

func TestRunnerProducerLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := mocksfoundation.NewApplication(t)
	config := newApplicationConfig(map[string]any{
		"dbos.enabled": true,
		"dbos.role":    RoleProducer,
	})
	runtime := &Runtime{
		app: app,
		config: Config{
			Role:    RoleProducer,
			AppName: "goravel-dbos-runner",
			// Leave the driver empty in this narrow runner test so it exercises
			// DBOS client preparation without starting the separately tested
			// package-owned experimental SQLite relay.
			DatabaseDriver:  "",
			DatabaseURL:     "sqlite:" + filepath.Join(t.TempDir(), "producer.db"),
			DatabaseSchema:  "dbos",
			LogLevel:        "error",
			StartupTimeout:  10 * time.Second,
			ShutdownTimeout: 10 * time.Second,
		},
	}

	app.EXPECT().MakeConfig().Return(config).Once()
	app.EXPECT().Make(Binding).Return(runtime, nil).Twice()
	app.EXPECT().Context().Return(ctx).Maybe()
	app.EXPECT().MakeLog().Return(nil).Maybe()
	runner := &runner{app: app}

	require.Equal(t, RunnerSignature, runner.Signature())
	require.True(t, runner.ShouldRun())
	done := make(chan error, 1)
	go func() { done <- runner.Run() }()
	require.Eventually(t, func() bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.client != nil
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.NoError(t, runner.Shutdown())
	require.Equal(t, -100, runner.ShutdownPriority())
}

type recordingWorkflowHandle struct{ id string }

func (h recordingWorkflowHandle) GetResult(...dbossdk.GetResultOption) (any, error) {
	return nil, nil
}
func (h recordingWorkflowHandle) GetStatus() (dbossdk.WorkflowStatus, error) {
	return dbossdk.WorkflowStatus{}, nil
}
func (h recordingWorkflowHandle) GetWorkflowID() string { return h.id }

type recordingClient struct {
	dbossdk.Client
	queue        string
	workflowName string
	input        any
	optionCount  int
	err          error
}

func (c *recordingClient) Enqueue(_ dbossdk.Client, queue, workflowName string, input any, options ...dbossdk.EnqueueOption) (dbossdk.WorkflowHandle[any], error) {
	c.queue = queue
	c.workflowName = workflowName
	c.input = input
	c.optionCount = len(options)
	if c.err != nil {
		return nil, c.err
	}
	return recordingWorkflowHandle{id: "workflow-1"}, nil
}

type enqueueRuntime struct {
	client dbossdk.Client
	err    error
}

func (r enqueueRuntime) Client() (dbossdk.Client, error) { return r.client, r.err }
func (enqueueRuntime) Role() string                      { return RoleProducer }
func (enqueueRuntime) ApplicationVersion() string        { return "2026.08.1" }
func (enqueueRuntime) ApplicationName() string           { return "billing" }
func (enqueueRuntime) DatabaseConnection() string        { return "primary" }
func (enqueueRuntime) DatabaseDriver() string            { return "postgres" }
func (enqueueRuntime) DatabaseSchema() string            { return "dbos" }

func TestEnqueueUsesStableTypedDefinitionAndVersion(t *testing.T) {
	client := &recordingClient{}
	definition := Define[map[string]string, string]("billing.invoice.v1", "billing")

	handle, err := Enqueue(enqueueRuntime{client: client}, definition, map[string]string{"invoice_id": "123"}, dbossdk.WithEnqueueWorkflowID("invoice:123"))

	require.NoError(t, err)
	require.Equal(t, "workflow-1", handle.GetWorkflowID())
	require.Equal(t, "billing", client.queue)
	require.Equal(t, "billing.invoice.v1", client.workflowName)
	require.Equal(t, map[string]string{"invoice_id": "123"}, client.input)
	require.Equal(t, 3, client.optionCount, "caller options plus the package application version and name")
}

func TestEnqueueFailsClosed(t *testing.T) {
	definition := Define[string, string]("example.v1", "example")
	_, err := Enqueue[string, string](nil, definition, "input")
	require.ErrorContains(t, err, "runtime is required")

	_, err = Enqueue(enqueueRuntime{err: context.Canceled}, definition, "input")
	require.ErrorIs(t, err, context.Canceled)

	client := &recordingClient{err: context.DeadlineExceeded}
	_, err = Enqueue(enqueueRuntime{client: client}, definition, "input")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, definition.Name())
}

func TestServiceProviderRegisterBindsRuntimeFactory(t *testing.T) {
	previousApp := App
	t.Cleanup(func() { App = previousApp })
	app := mocksfoundation.NewApplication(t)
	var factory func(contractsfoundation.Application) (any, error)
	app.EXPECT().Singleton(Binding, mock.Anything).Run(func(_ any, callback func(contractsfoundation.Application) (any, error)) {
		factory = callback
	}).Once()

	(&ServiceProvider{}).Register(app)

	require.NotNil(t, factory)
	require.Same(t, app, App)
}

package goraveldbos

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func postgresWorkflowEnvironment(t *testing.T) (context.Context, Config) {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv(integrationDatabaseEnvironment))
	if databaseURL == "" {
		t.Skipf("set %s to run PostgreSQL integration tests", integrationDatabaseEnvironment)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	schema := "goravel_dbos_workflow_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	connection, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		_, err := connection.Exec(cleanup, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		require.NoError(t, err)
		require.NoError(t, connection.Close(cleanup))
	})
	return ctx, Config{
		AppName: "goravel-dbos-upgrade", ApplicationVersion: "upgrade-v1",
		DatabaseConnection: "primary", DatabaseDriver: "postgres",
		DatabaseURL: databaseURL, DatabaseSchema: schema,
		PoolMaxConns: 10, PoolMaxConnLifetime: time.Hour,
		PoolMaxConnIdleTime: 5 * time.Minute, PoolConnectTimeout: 10 * time.Second,
		LogLevel: "error", StartupTimeout: 30 * time.Second,
		ShutdownTimeout: 10 * time.Second, SchedulerPollingInterval: 30 * time.Second,
	}
}

func workflowRuntime(t *testing.T, ctx context.Context, config Config, role string) *Runtime {
	t.Helper()
	app := mocksfoundation.NewApplication(t)
	app.EXPECT().Context().Return(ctx).Maybe()
	app.EXPECT().MakeLog().Return(nil).Maybe()
	config.Role = role
	runtime := &Runtime{app: app, config: config}
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown()) })
	return runtime
}

func TestPostgresParallelWorkflowsAndHumanInputSurviveRestart(t *testing.T) {
	ctx, config := postgresWorkflowEnvironment(t)
	team := Define[string, string]("team.v1", "teams")
	agent := Define[string, string]("agent.v1", "agents")
	var preparations atomic.Int32
	registrar := RegistrarFunc(func(registry *Registry) error {
		// Separate queues let children run even when every parent slot is waiting.
		if _, err := registry.Queue(team.Queue(), dbossdk.WithGlobalConcurrency(1)); err != nil {
			return err
		}
		if _, err := registry.Queue(agent.Queue(), dbossdk.WithGlobalConcurrency(2)); err != nil {
			return err
		}
		if err := RegisterWorkflow(registry, agent, func(ctx dbossdk.Context, input string) (string, error) {
			if err := dbossdk.SetEvent(ctx, "started", true); err != nil {
				return "", err
			}
			_, err := dbossdk.Recv[string](ctx, "release", time.Minute)
			return input, err
		}); err != nil {
			return err
		}
		return RegisterWorkflow(registry, team, func(ctx dbossdk.Context, input string) (string, error) {
			prepared, err := dbossdk.RunAsStep(ctx, func(context.Context) (string, error) {
				preparations.Add(1)
				return input + ":prepared", nil
			}, dbossdk.WithStepName("prepare"))
			if err != nil {
				return "", err
			}
			handles := make([]dbossdk.WorkflowHandle[string], 0, 2)
			ids := make([]string, 0, 2)
			for _, task := range []string{"a", "b"} {
				// The workflow context checkpoints child creation and lineage.
				handle, err := dbossdk.Enqueue[string](ctx, agent.Queue(), agent.Name(), task)
				if err != nil {
					return "", err
				}
				handles = append(handles, handle)
				ids = append(ids, handle.GetWorkflowID())
			}
			if err := dbossdk.SetEvent(ctx, "children", ids); err != nil {
				return "", err
			}
			for _, handle := range handles {
				result, err := handle.GetResult()
				if err != nil {
					return "", err
				}
				prepared += ":" + result
			}
			if err := dbossdk.SetEvent(ctx, "review", prepared); err != nil {
				return "", err
			}
			approval, err := dbossdk.Recv[string](ctx, "approval", time.Hour)
			return prepared + ":" + approval, err
		})
	})
	worker := workflowRuntime(t, ctx, config, RoleWorker)
	require.NoError(t, worker.AddRegistrar(registrar))
	require.NoError(t, worker.Start())
	producer := workflowRuntime(t, ctx, config, RoleProducer)
	handle, err := Enqueue(producer, team, "team")
	require.NoError(t, err)
	client, err := producer.Client()
	require.NoError(t, err)
	ids, err := dbossdk.GetEvent[[]string](client, handle.GetWorkflowID(), "children", 20*time.Second)
	require.NoError(t, err)
	require.Len(t, ids, 2)
	// Both branches must start before either is released: this proves parallelism.
	for _, id := range ids {
		started, err := dbossdk.GetEvent[bool](client, id, "started", 20*time.Second)
		require.NoError(t, err)
		require.True(t, started)
		child, err := dbossdk.RetrieveWorkflow[string](client, id)
		require.NoError(t, err)
		status, err := child.GetStatus()
		require.NoError(t, err)
		require.Equal(t, handle.GetWorkflowID(), status.ParentWorkflowID)
	}
	for _, id := range ids {
		require.NoError(t, dbossdk.Send(client, id, "done", "release"))
	}
	review, err := dbossdk.GetEvent[string](client, handle.GetWorkflowID(), "review", 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, "team:prepared:a:b", review)
	require.NoError(t, worker.Shutdown())
	status, err := handle.GetStatus()
	require.NoError(t, err)
	require.Equal(t, dbossdk.WorkflowStatusPending, status.Status)
	// A human can reply while no worker is running; retries use the same key.
	for range 2 {
		require.NoError(t, dbossdk.Send(client, handle.GetWorkflowID(), "approved", "approval",
			dbossdk.WithIdempotencyKey("review-1")))
	}
	restarted := workflowRuntime(t, ctx, config, RoleWorker)
	require.NoError(t, restarted.AddRegistrar(registrar))
	require.NoError(t, restarted.Start())
	result, err := handle.GetResult(dbossdk.WithHandleTimeout(20 * time.Second))
	require.NoError(t, err)
	require.Equal(t, "team:prepared:a:b:approved", result)
	require.Equal(t, int32(1), preparations.Load(), "completed side effects must replay from checkpoints")
}

func TestPostgresUpgradeFromDBOSV1(t *testing.T) {
	ctx, config := postgresWorkflowEnvironment(t)
	fixture := os.Getenv("GORAVEL_DBOS_V1_FIXTURE")
	require.NotEmpty(t, fixture, "build testdata/upgrade in CI and set GORAVEL_DBOS_V1_FIXTURE")
	output, err := exec.CommandContext(ctx, fixture, config.DatabaseSchema).CombinedOutput()
	require.NoError(t, err, "%s", output)
	producer := workflowRuntime(t, ctx, config, RoleProducer)
	require.NoError(t, producer.MigrateSystemDatabase())
	require.NoError(t, producer.Prepare())
	client, err := producer.Client()
	require.NoError(t, err)
	worker := workflowRuntime(t, ctx, config, RoleWorker)
	require.NoError(t, worker.AddRegistrar(RegistrarFunc(func(registry *Registry) error {
		if _, err := registry.Queue("upgrade"); err != nil {
			return err
		}
		return RegisterWorkflow(registry, Define[string, string]("upgrade.workflow.v1", "upgrade"),
			func(ctx dbossdk.Context, _ string) (string, error) {
				prepared, err := dbossdk.RunAsStep(ctx, func(context.Context) (string, error) {
					return "", errors.New("a completed DBOS 1.0 step was executed again")
				}, dbossdk.WithStepName("prepare"))
				if err != nil {
					return "", err
				}
				if err := dbossdk.SetEvent(ctx, "ready", true); err != nil {
					return "", err
				}
				approval, err := dbossdk.Recv[string](ctx, "approval", time.Hour)
				return prepared + ":" + approval, err
			})
	})))
	require.NoError(t, worker.Start())
	for _, source := range []string{"sdk", "sql"} {
		id := "upgrade-" + source
		require.NoError(t, dbossdk.Send(client, id, "approved", "approval"))
		handle, err := dbossdk.RetrieveWorkflow[string](client, id)
		require.NoError(t, err)
		result, err := handle.GetResult(dbossdk.WithHandleTimeout(20 * time.Second))
		require.NoError(t, err)
		require.Equal(t, source+":prepared:approved", result)
	}
}

func TestPostgresProducerApplicationOwnership(t *testing.T) {
	ctx, config := postgresWorkflowEnvironment(t)
	producer := workflowRuntime(t, ctx, config, RoleProducer)
	require.NoError(t, producer.Prepare())
	definition := Define[string, string]("owned.v1", "owned")
	handle, err := Enqueue(producer, definition, "input",
		dbossdk.WithEnqueueApplicationName("another-app"))
	require.NoError(t, err)
	status, err := handle.GetStatus()
	require.NoError(t, err)
	require.Equal(t, config.AppName, status.ApplicationName)
	config.AppName = "another-app"
	other := workflowRuntime(t, ctx, config, RoleProducer)
	client, err := other.Client()
	require.NoError(t, err)
	workflows, err := dbossdk.ListWorkflows(client)
	require.NoError(t, err)
	require.Empty(t, workflows, "a producer must not act as a nameless client across applications")
}

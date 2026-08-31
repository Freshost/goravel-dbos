package goraveldbos

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type sqliteTransactionScanner struct{ tx *sql.Tx }

func (s sqliteTransactionScanner) InTransaction() bool { return true }
func (s sqliteTransactionScanner) Driver() string      { return "sqlite" }
func (s sqliteTransactionScanner) ExecRaw(query string, values ...any) (int64, error) {
	result, err := s.tx.Exec(query, values...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
func (s sqliteTransactionScanner) ScanRaw(query string, destination any, values ...any) error {
	var workflowID, workflowName, queueName string
	if err := s.tx.QueryRow(query, values...).Scan(&workflowID, &workflowName, &queueName); err != nil {
		return err
	}
	value := reflect.ValueOf(destination).Elem()
	value.FieldByName("WorkflowID").SetString(workflowID)
	value.FieldByName("WorkflowName").SetString(workflowName)
	value.FieldByName("QueueName").SetString(queueName)
	return nil
}

const integrationDatabaseEnvironment = "GORAVEL_DBOS_TEST_DATABASE_URL"

type pgxTransactionScanner struct {
	ctx context.Context
	tx  pgx.Tx
}

func (s pgxTransactionScanner) InTransaction() bool { return true }
func (s pgxTransactionScanner) Driver() string      { return "postgres" }
func (s pgxTransactionScanner) ScanRaw(query string, destination any, values ...any) error {
	var workflowID string
	if err := s.tx.QueryRow(s.ctx, positionalPostgresQuery(query), values...).Scan(&workflowID); err != nil {
		return err
	}
	reflect.ValueOf(destination).Elem().FieldByName("WorkflowID").SetString(workflowID)
	return nil
}
func (s pgxTransactionScanner) ExecRaw(query string, values ...any) (int64, error) {
	result, err := s.tx.Exec(s.ctx, positionalPostgresQuery(query), values...)
	return result.RowsAffected(), err
}

func positionalPostgresQuery(query string) string {
	var builder strings.Builder
	index := 1
	for _, character := range query {
		if character == '?' {
			_, _ = fmt.Fprintf(&builder, "$%d", index)
			index++
		} else {
			builder.WriteRune(character)
		}
	}
	return builder.String()
}

func TestPostgresTransactionalEnqueueCommitAndRollback(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv(integrationDatabaseEnvironment))
	if databaseURL == "" {
		t.Skipf("set %s to run the PostgreSQL integration test", integrationDatabaseEnvironment)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	schema := "goravel_dbos_integration_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	connection, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = connection.Exec(cleanupContext, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		_ = connection.Close(cleanupContext)
	})

	client, err := dbossdk.NewContext(ctx, dbossdk.Config{
		AppName:                "goravel-dbos-integration",
		ApplicationVersion:     "integration-v1",
		DatabaseURL:            databaseURL,
		DatabaseSchema:         schema,
		SystemDBStartupTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbossdk.Shutdown(client, 10*time.Second) })

	runtime := fakeRuntime{version: "integration-v1", schema: schema}
	definition := Define[map[string]string, struct{}]("integration.workflow.v1", "integration")

	commitTx, err := connection.Begin(ctx)
	require.NoError(t, err)
	committedID, err := enqueueWithTransactionScanner(
		runtime,
		pgxTransactionScanner{ctx: ctx, tx: commitTx},
		definition,
		map[string]string{"invoice_id": "123"},
		WithTransactionWorkflowID("integration-commit"),
	)
	require.NoError(t, err)
	require.NoError(t, commitTx.Commit(ctx))

	handle, err := dbossdk.RetrieveWorkflow[any](client, committedID)
	require.NoError(t, err)
	status, err := handle.GetStatus()
	require.NoError(t, err)
	require.Equal(t, dbossdk.WorkflowStatusEnqueued, status.Status)
	require.Equal(t, "integration-v1", status.ApplicationVersion)
	require.Equal(t, "integration", status.QueueName)

	firstRegistry := newRegistry(client)
	require.NoError(t, firstRegistry.Schedule(dbossdk.ScheduleSpec{
		ScheduleName: "integration-schedule",
		Schedule:     "0 */5 * * * *",
		WorkflowName: definition.Name(),
		QueueName:    definition.Queue(),
	}))
	require.NoError(t, firstRegistry.applySchedules())

	restartedRegistry := newRegistry(client)
	require.NoError(t, restartedRegistry.Schedule(dbossdk.ScheduleSpec{
		ScheduleName: "integration-schedule",
		Schedule:     "0 */10 * * * *",
		WorkflowName: definition.Name(),
		QueueName:    definition.Queue(),
	}))
	require.NoError(t, restartedRegistry.applySchedules())
	schedule, err := dbossdk.GetSchedule(client, "integration-schedule")
	require.NoError(t, err)
	require.Equal(t, "0 */10 * * * *", schedule.Schedule)

	rollbackTx, err := connection.Begin(ctx)
	require.NoError(t, err)
	rolledBackID, err := enqueueWithTransactionScanner(
		runtime,
		pgxTransactionScanner{ctx: ctx, tx: rollbackTx},
		definition,
		map[string]string{"invoice_id": "456"},
		WithTransactionWorkflowID("integration-rollback"),
	)
	require.NoError(t, err)
	require.NoError(t, rollbackTx.Rollback(ctx))

	_, err = dbossdk.RetrieveWorkflow[any](client, rolledBackID)
	require.Error(t, err)
}

func TestPostgresProducerWorkerRuntimeLifecycle(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv(integrationDatabaseEnvironment))
	if databaseURL == "" {
		t.Skipf("set %s to run the PostgreSQL integration test", integrationDatabaseEnvironment)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	schema := "goravel_dbos_runtime_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	admin, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupContext, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		_ = admin.Close(cleanupContext)
	})

	baseConfig := Config{
		AppName:                  "goravel-dbos-runtime-integration",
		ApplicationVersion:       "integration-v1",
		DatabaseConnection:       "primary",
		DatabaseDriver:           "postgres",
		DatabaseURL:              databaseURL,
		DatabaseSchema:           schema,
		LogLevel:                 "error",
		StartupTimeout:           30 * time.Second,
		ShutdownTimeout:          10 * time.Second,
		SchedulerPollingInterval: 30 * time.Second,
	}
	definition := Define[string, string]("integration.echo.v1", "integration-runtime")

	workerApp := mocksfoundation.NewApplication(t)
	workerApp.EXPECT().Context().Return(ctx).Maybe()
	workerApp.EXPECT().MakeLog().Return(nil).Maybe()
	workerConfig := baseConfig
	workerConfig.Role = RoleWorker
	worker := &Runtime{app: workerApp, config: workerConfig}
	require.NoError(t, worker.AddRegistrar(RegistrarFunc(func(registry *Registry) error {
		if _, err := registry.Queue(definition.Queue(), dbossdk.WithGlobalConcurrency(1)); err != nil {
			return err
		}
		return RegisterWorkflow(registry, definition, func(_ dbossdk.Context, input string) (string, error) {
			return "processed:" + input, nil
		})
	})))
	require.NoError(t, worker.Start())
	t.Cleanup(func() { require.NoError(t, worker.Shutdown()) })

	producerApp := mocksfoundation.NewApplication(t)
	producerApp.EXPECT().Context().Return(ctx).Maybe()
	producerApp.EXPECT().MakeLog().Return(nil).Maybe()
	producerConfig := baseConfig
	producerConfig.Role = RoleProducer
	producer := &Runtime{app: producerApp, config: producerConfig}
	t.Cleanup(func() { require.NoError(t, producer.Shutdown()) })

	handle, err := Enqueue(producer, definition, "hello", dbossdk.WithEnqueueWorkflowID("runtime-lifecycle"))
	require.NoError(t, err)
	result, err := handle.GetResult(dbossdk.WithHandleTimeout(20 * time.Second))
	require.NoError(t, err)
	require.Equal(t, "processed:hello", result)

	require.ErrorIs(t, worker.AddRegistrar(RegistrarFunc(func(*Registry) error { return nil })), ErrAlreadyStarted)
	require.NoError(t, producer.Shutdown())
	_, err = producer.Client()
	require.ErrorContains(t, err, "runtime is closed")
	require.NoError(t, worker.Shutdown())
}

func TestSQLiteTransactionalDispatchCommitRollbackAndIdempotentDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	databasePath := t.TempDir() + "/application.db"
	database, err := sql.Open("sqlite", databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = database.Close() })

	_, err = database.ExecContext(ctx, sqliteMigrationsTableSQL)
	require.NoError(t, err)
	for _, statement := range sqliteOutboxMigrations[0].statements {
		_, err = database.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	_, err = database.ExecContext(ctx, `CREATE TABLE invoices (id TEXT PRIMARY KEY, state TEXT NOT NULL)`)
	require.NoError(t, err)

	runtime := fakeRuntime{version: "sqlite-v1", schema: "dbos"}
	definition := Define[map[string]string, string]("sqlite.workflow.v1", "sqlite-queue")

	rollbackTx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = rollbackTx.ExecContext(ctx, `INSERT INTO invoices(id, state) VALUES (?, ?)`, "rollback", "paid")
	require.NoError(t, err)
	_, err = enqueueWithTransactionScanner(runtime, sqliteTransactionScanner{tx: rollbackTx}, definition,
		map[string]string{"invoice_id": "rollback"}, WithTransactionWorkflowID("sqlite-rollback"))
	require.NoError(t, err)
	require.NoError(t, rollbackTx.Rollback())
	var rollbackCount int
	require.NoError(t, database.QueryRowContext(ctx, `SELECT COUNT(*) FROM goravel_dbos_outbox WHERE workflow_id = ?`, "sqlite-rollback").Scan(&rollbackCount))
	require.Zero(t, rollbackCount)

	commitTx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = commitTx.ExecContext(ctx, `INSERT INTO invoices(id, state) VALUES (?, ?)`, "commit", "paid")
	require.NoError(t, err)
	workflowID, err := enqueueWithTransactionScanner(runtime, sqliteTransactionScanner{tx: commitTx}, definition,
		map[string]string{"invoice_id": "commit"}, WithTransactionWorkflowID("sqlite-commit"))
	require.NoError(t, err)
	require.NoError(t, commitTx.Commit())

	var row sqliteOutboxRow
	require.NoError(t, database.QueryRowContext(ctx, `SELECT workflow_id, workflow_name, queue_name, input_json, options_json, application_version, attempts FROM goravel_dbos_outbox WHERE workflow_id = ?`, workflowID).
		Scan(&row.WorkflowID, &row.WorkflowName, &row.QueueName, &row.InputJSON, &row.OptionsJSON, &row.ApplicationVersion, &row.Attempts))

	client, err := dbossdk.NewContext(ctx, dbossdk.Config{
		AppName:                "goravel-dbos-sqlite-integration",
		ApplicationVersion:     "sqlite-v1",
		DatabaseURL:            "sqlite:" + databasePath,
		SystemDBStartupTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbossdk.Shutdown(client, 10*time.Second) })
	registry := newRegistry(client)
	_, err = registry.Queue(definition.Queue())
	require.NoError(t, err)
	require.NoError(t, RegisterWorkflow(registry, definition, func(_ dbossdk.Context, input map[string]string) (string, error) {
		return input["invoice_id"], nil
	}))
	require.NoError(t, dbossdk.Launch(client))
	require.NoError(t, deliverSQLiteOutboxRow(client, row, time.Now()))
	// Simulate a crash after DBOS accepted the enqueue but before the outbox ack.
	require.NoError(t, deliverSQLiteOutboxRow(client, row, time.Now()))

	handle, err := dbossdk.RetrieveWorkflow[string](client, workflowID)
	require.NoError(t, err)
	status, err := handle.GetStatus()
	require.NoError(t, err)
	require.Equal(t, dbossdk.WorkflowStatusEnqueued, status.Status)
	require.Equal(t, "sqlite-v1", status.ApplicationVersion)
	require.Equal(t, definition.Queue(), status.QueueName)
	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "commit", result)
}

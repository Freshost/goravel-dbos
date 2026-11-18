package goraveldbos

import (
	"bytes"
	"context"
	"errors"
	"log"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	contractsdatabase "github.com/goravel/framework/contracts/database"
	contractsdriver "github.com/goravel/framework/contracts/database/driver"
	contractsorm "github.com/goravel/framework/contracts/database/orm"
	databasegorm "github.com/goravel/framework/database/gorm"
	databaseorm "github.com/goravel/framework/database/orm"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	goravelsqlite "github.com/goravel/sqlite"
	sqlitecontracts "github.com/goravel/sqlite/contracts"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

func sqliteGoravelRuntime(t *testing.T) (*Runtime, contractsorm.Orm, *gorm.DB, *bytes.Buffer, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	dsn := "file:" + filepath.Join(t.TempDir(), "application.db") + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	config := newApplicationConfig(map[string]any{
		"database.default":                     "sqlite",
		"database.connections.sqlite.database": "unused-base.db",
		"database.connections.sqlite.write":    []sqlitecontracts.Config{{Dsn: dsn}},
		"dbos.app_name":                        "sqlite-integration",
		"dbos.application_version":             "sqlite-v1",
		"dbos.sqlite.experimental":             true,
		"dbos.sqlite.relay_poll_milliseconds":  20,
		"dbos.sqlite.retry_base_milliseconds":  20,
		"dbos.sqlite.retry_max_seconds":        1,
		"dbos.shutdown_timeout_seconds":        5,
	})
	driver := goravelsqlite.NewSqlite(config, nil, "sqlite")
	config.Add("database.connections.sqlite.via", func() (contractsdriver.Driver, error) { return driver, nil })
	var logs bytes.Buffer
	logger := gormlogger.New(log.New(&logs, "", 0), gormlogger.Config{LogLevel: gormlogger.Info})
	db, err := gorm.Open(driver.Pool().Writers[0].Dialector, &gorm.Config{SkipDefaultTransaction: true, Logger: logger})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	native := driver.Pool().Writers[0]
	query := databasegorm.NewQuery(ctx, config, native, db, driver.Grammar(), nil, nil, nil, nil)
	ormService := databaseorm.NewOrm(ctx, config, "sqlite", native, query,
		map[string]contractsorm.Query{"sqlite": query}, nil, nil, nil, nil)
	app := mocksfoundation.NewApplication(t)
	app.EXPECT().MakeConfig().Return(config).Maybe()
	app.EXPECT().MakeOrm().Return(ormService).Maybe()
	app.EXPECT().Context().Return(ctx).Maybe()
	app.EXPECT().MakeLog().Return(nil).Maybe()
	runtime, err := NewRuntime(app)
	require.NoError(t, err)
	require.Equal(t, "sqlite:"+dsn, runtime.config.DatabaseURL, "DBOS must use the resolved writer, not base settings")
	app.EXPECT().Make(Binding).Return(runtime, nil).Maybe()
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown()) })
	return runtime, ormService, db, &logs, cancel
}

type retryOnceClient struct {
	dbossdk.Client
	attempts atomic.Int32
}

func (c *retryOnceClient) Enqueue(_ dbossdk.Client, queue, name string, input any, options ...dbossdk.EnqueueOption) (dbossdk.WorkflowHandle[any], error) {
	if c.attempts.Add(1) == 1 {
		return nil, errors.New("temporary delivery failure")
	}
	return c.Client.Enqueue(c.Client, queue, name, input, options...)
}

func TestSQLiteGoravelRunnerDeliversCommittedWorkAndRetries(t *testing.T) {
	runtime, ormService, db, _, cancel := sqliteGoravelRuntime(t)
	require.NoError(t, db.Exec("CREATE TABLE invoices (id TEXT PRIMARY KEY)").Error)
	definition := Define[string, string]("sqlite.relay.v1", "sqlite-relay")
	worker := &Runtime{app: runtime.app, config: runtime.config}
	worker.config.Role = RoleWorker
	require.NoError(t, worker.AddRegistrar(RegistrarFunc(func(registry *Registry) error {
		if _, err := registry.Queue(definition.Queue()); err != nil {
			return err
		}
		return RegisterWorkflow(registry, definition, func(_ dbossdk.Context, input string) (string, error) { return "processed:" + input, nil })
	})))
	require.NoError(t, worker.Start())
	t.Cleanup(func() { require.NoError(t, worker.Shutdown()) })
	rollback := errors.New("rollback")
	for _, id := range []string{"committed", "rolled-back"} {
		err := ormService.Transaction(func(tx contractsorm.Query) error {
			if _, err := tx.Exec("INSERT INTO invoices (id) VALUES (?)", id); err != nil {
				return err
			}
			if _, err := EnqueueInTransaction(runtime, tx, definition, id, WithTransactionWorkflowID(id)); err != nil {
				return err
			}
			if id == "rolled-back" {
				return rollback
			}
			return nil
		})
		if id == "rolled-back" {
			require.ErrorIs(t, err, rollback)
		} else {
			require.NoError(t, err)
		}
	}
	client, err := runtime.Client()
	require.NoError(t, err)
	retryClient := &retryOnceClient{Client: client}
	runtime.client = retryClient
	runner := &runner{app: runtime.app}
	done := make(chan error, 1)
	go func() { done <- runner.Run() }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
		require.NoError(t, runner.Shutdown())
	})
	var delivery struct {
		Attempts      int
		DeliveredAtMS *int64
		LockedBy      *string
	}
	require.Eventually(t, func() bool {
		return db.Raw("SELECT attempts, delivered_at_ms, locked_by FROM goravel_dbos_outbox WHERE workflow_id = ?", "committed").Scan(&delivery).Error == nil && delivery.DeliveredAtMS != nil
	}, 15*time.Second, 20*time.Millisecond)
	handle, err := dbossdk.RetrieveWorkflow[string](client, "committed")
	require.NoError(t, err)
	result, err := handle.GetResult(dbossdk.WithHandleTimeout(15 * time.Second))
	require.NoError(t, err)
	require.Equal(t, "processed:committed", result)
	require.NoError(t, runner.Shutdown())
	require.GreaterOrEqual(t, retryClient.attempts.Load(), int32(2))
	require.GreaterOrEqual(t, delivery.Attempts, 2)
	require.NotNil(t, delivery.DeliveredAtMS)
	require.Nil(t, delivery.LockedBy)
	var count int64
	require.NoError(t, db.Raw("SELECT count(*) FROM invoices WHERE id = 'rolled-back'").Scan(&count).Error)
	require.Zero(t, count)
	require.NoError(t, db.Raw("SELECT count(*) FROM goravel_dbos_outbox WHERE workflow_id = 'rolled-back'").Scan(&count).Error)
	require.Zero(t, count)
}

func TestTransactionalQueriesKeepPayloadsOutOfSQLLogs(t *testing.T) {
	runtime, ormService, db, logs, _ := sqliteGoravelRuntime(t)
	definition := Define[string, string]("private.v1", "private")
	rollback := errors.New("rollback")
	err := ormService.Transaction(func(tx contractsorm.Query) error {
		_, err := EnqueueInTransaction(runtime, tx, definition, "private-success-payload")
		return err
	})
	require.NoError(t, err)
	err = ormService.Transaction(func(tx contractsorm.Query) error {
		if _, err := tx.Exec("DROP TABLE goravel_dbos_outbox"); err != nil {
			return err
		}
		_, err := EnqueueInTransaction(runtime, tx, definition, "private-failure-payload")
		require.Error(t, err)
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	require.NotContains(t, logs.String(), "private-success-payload")
	require.NotContains(t, logs.String(), "private-failure-payload")
	var value string
	require.NoError(t, db.Raw("SELECT ?", "caller-logging-still-enabled").Scan(&value).Error)
	require.Contains(t, logs.String(), "caller-logging-still-enabled")
}

func TestTransactionalEnqueueRejectsAnotherWriterPool(t *testing.T) {
	first, _, _, _, _ := sqliteGoravelRuntime(t)
	_, other, _, _, _ := sqliteGoravelRuntime(t)
	err := other.Transaction(func(tx contractsorm.Query) error {
		_, err := EnqueueInTransaction(first, tx, Define[string, string]("example.v1", "example"), "private")
		return err
	})
	require.ErrorContains(t, err, "does not belong to the configured native writer pool")
}

type poolDriver struct {
	contractsdriver.Driver
	pool contractsdatabase.Pool
}

func (d poolDriver) Pool() contractsdatabase.Pool { return d.pool }

func TestRuntimeUsesResolvedPostgresWriterAndRejectsMultipleWriters(t *testing.T) {
	native := contractsdatabase.Config{Driver: "postgres", Dsn: "postgresql://writer:private@writer/billing?sslmode=disable"}
	for _, writers := range [][]contractsdatabase.Config{{native}, {native, native}} {
		app := mocksfoundation.NewApplication(t)
		config := newApplicationConfig(map[string]any{
			"database.default": "primary", "dbos.app_name": "billing", "dbos.application_version": "v1",
			"database.connections.primary.dsn": "postgresql://wrong/base",
			"database.connections.primary.via": func() (contractsdriver.Driver, error) {
				return poolDriver{pool: contractsdatabase.Pool{Writers: writers}}, nil
			},
		})
		app.EXPECT().MakeConfig().Return(config).Once()
		runtime, err := NewRuntime(app)
		if len(writers) == 1 {
			require.NoError(t, err)
			require.Equal(t, native.Dsn, runtime.config.DatabaseURL)
		} else {
			require.ErrorContains(t, err, "exactly one writer")
		}
	}
}

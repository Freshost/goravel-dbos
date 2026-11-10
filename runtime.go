package goraveldbos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	_ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite"
	contractsfoundation "github.com/goravel/framework/contracts/foundation"
	"github.com/jackc/pgx/v5/pgxpool"

	packagecontracts "github.com/freshost/goravel-dbos/contracts"
)

var (
	ErrDisabled       = errors.New("goravel-dbos: DBOS is disabled")
	ErrNotExecutor    = errors.New("goravel-dbos: current process is not a DBOS executor")
	ErrAlreadyStarted = errors.New("goravel-dbos: runtime is already started")
)

// Runtime owns one process's DBOS Client or executor Context and their complete
// lifecycle. It does not own application workflows; those arrive as registrars.
type Runtime struct {
	app         contractsfoundation.Application
	config      Config
	mu          sync.Mutex
	client      dbossdk.Client
	executor    dbossdk.Context
	registrars  []Registrar
	started     bool
	closed      bool
	relayCancel context.CancelFunc
	relayDone   chan struct{}
}

var _ packagecontracts.Runtime = (*Runtime)(nil)
var _ packagecontracts.ApplicationRuntime = (*Runtime)(nil)

func NewRuntime(app contractsfoundation.Application) (*Runtime, error) {
	if app == nil {
		return nil, errors.New("goravel-dbos: application is required")
	}
	config := app.MakeConfig()
	driver := ""
	if config != nil && config.GetBool("dbos.enabled", true) && strings.ToLower(strings.TrimSpace(config.GetString("dbos.role", RoleProducer))) != RoleDisabled {
		ormService := app.MakeOrm()
		if ormService == nil {
			return nil, errors.New("goravel-dbos: Goravel ORM service is unavailable")
		}
		driver = ormService.Connection(config.GetString("database.default")).Config().Driver
	}
	cfg, err := loadConfig(config, driver)
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{app: app, config: cfg}
	if cfg.DatabaseDriver == "sqlite" {
		if logger := app.MakeLog(); logger != nil {
			logger.Warning("goravel-dbos: SQLite support is experimental and not recommended for production; Goravel and DBOS currently use different SQLite driver implementations")
		}
		if err := runtime.migrateSQLiteOutbox(); err != nil {
			return nil, err
		}
	}
	return runtime, nil
}

func (r *Runtime) Role() string               { return r.config.Role }
func (r *Runtime) ApplicationName() string    { return r.config.AppName }
func (r *Runtime) ApplicationVersion() string { return r.config.ApplicationVersion }
func (r *Runtime) DatabaseConnection() string { return r.config.DatabaseConnection }
func (r *Runtime) DatabaseDriver() string     { return r.config.DatabaseDriver }
func (r *Runtime) DatabaseSchema() string     { return r.config.DatabaseSchema }

// Prepare initializes the DBOS system database and applies SDK-owned schema
// migrations without launching workflow execution. It is safe to call more
// than once. Applications should run dbos:migrate as an explicit deployment
// step before processes that use transaction-bound enqueue accept traffic.
func (r *Runtime) Prepare() error {
	// Standalone clients only verify the schema as of DBOS Go 1.8. Preserve
	// startup preparation through a separate migration-only context.
	if r.config.Role == RoleProducer {
		if err := r.MigrateSystemDatabase(); err != nil {
			return err
		}
	}
	_, err := r.Client()
	if err != nil {
		return fmt.Errorf("goravel-dbos: prepare system database: %w", err)
	}
	return nil
}

// MigrateSystemDatabase initializes or upgrades the DBOS SDK-owned system
// schema without starting a producer notification listener or workflow
// executor. It is intended for short-lived release commands and does not alter
// the lifecycle state of this Runtime.
func (r *Runtime) MigrateSystemDatabase() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.config.Role == RoleDisabled {
		return ErrDisabled
	}
	if r.closed {
		return errors.New("goravel-dbos: runtime is closed")
	}

	databaseURL, systemDBPool, err := r.databaseInputs()
	if err != nil {
		return err
	}
	migrationContext, err := dbossdk.NewContext(r.app.Context(), dbossdk.Config{
		AppName:                r.config.AppName,
		DatabaseURL:            databaseURL,
		SystemDBPool:           systemDBPool,
		DatabaseSchema:         r.config.DatabaseSchema,
		Logger:                 newDBOSLogger(r.app.MakeLog(), r.config.LogLevel),
		SystemDBStartupTimeout: r.config.StartupTimeout,
	})
	if err != nil {
		if systemDBPool != nil {
			systemDBPool.Close()
		}
		return fmt.Errorf("goravel-dbos: migrate system database: %w", err)
	}
	if err := dbossdk.Shutdown(migrationContext, r.config.ShutdownTimeout); err != nil {
		return fmt.Errorf("goravel-dbos: close migration context: %w", err)
	}
	return nil
}

// AddRegistrar adds application-owned workflow registration. It is safe during
// Goravel provider Boot and rejected after executor startup begins.
func (r *Runtime) AddRegistrar(registrar Registrar) error {
	if registrar == nil {
		return errors.New("goravel-dbos: registrar is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("goravel-dbos: runtime is closed")
	}
	if r.started {
		return ErrAlreadyStarted
	}
	r.registrars = append(r.registrars, registrar)
	return nil
}

// Client returns a lazily-created standalone client in producer processes and
// the executor Context in worker processes. A standalone client requires an
// up-to-date schema: call MigrateSystemDatabase or Prepare before first use.
func (r *Runtime) Client() (dbossdk.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.config.Role == RoleDisabled {
		return nil, ErrDisabled
	}
	if r.closed {
		return nil, errors.New("goravel-dbos: runtime is closed")
	}
	if r.executor != nil {
		return r.executor, nil
	}
	if r.client != nil {
		return r.client, nil
	}
	if r.config.Role == RoleWorker || r.config.Role == RoleEmbedded {
		executor, err := r.newExecutor()
		if err != nil {
			return nil, err
		}
		r.executor = executor
		return executor, nil
	}
	client, err := r.newClient()
	if err != nil {
		return nil, fmt.Errorf("goravel-dbos: initialize producer client: %w", err)
	}
	r.client = client
	return client, nil
}

// Start constructs, registers and launches an executor runtime. Producer mode
// intentionally does nothing.
func (r *Runtime) Start() error {
	r.mu.Lock()
	if r.config.Role == RoleDisabled {
		r.mu.Unlock()
		return ErrDisabled
	}
	if r.config.Role == RoleProducer {
		r.mu.Unlock()
		return nil
	}
	if r.started {
		r.mu.Unlock()
		return ErrAlreadyStarted
	}
	if r.closed {
		r.mu.Unlock()
		return errors.New("goravel-dbos: runtime is closed")
	}
	r.started = true
	ctx := r.executor
	if ctx == nil {
		var err error
		ctx, err = r.newExecutor()
		if err != nil {
			r.mu.Unlock()
			return err
		}
		r.executor = ctx
	}
	registrars := append([]Registrar{}, r.registrars...)
	r.mu.Unlock()

	registry := newRegistry(ctx)
	for _, registrar := range registrars {
		if err := registrar.RegisterDBOS(registry); err != nil {
			r.closeFailedExecutor(ctx)
			return fmt.Errorf("goravel-dbos: register application workflows: %w", err)
		}
	}
	if err := registry.applySchedules(); err != nil {
		r.closeFailedExecutor(ctx)
		return err
	}
	if len(r.config.ListenQueues) > 0 {
		dbossdk.ListenQueues(ctx, r.config.ListenQueues...)
	}
	if err := dbossdk.Launch(ctx); err != nil {
		r.closeFailedExecutor(ctx)
		return fmt.Errorf("goravel-dbos: launch executor: %w", err)
	}

	for _, registrar := range registrars {
		if hook, ok := registrar.(AfterLauncher); ok {
			if err := hook.AfterDBOSLaunch(); err != nil {
				_ = r.Shutdown()
				return fmt.Errorf("goravel-dbos: after-launch hook: %w", err)
			}
		}
	}
	return nil
}

func (r *Runtime) newExecutor() (dbossdk.Context, error) {
	databaseURL, systemDBPool, err := r.databaseInputs()
	if err != nil {
		return nil, err
	}
	ctx, err := dbossdk.NewContext(r.app.Context(), dbossdk.Config{
		AppName:                  r.config.AppName,
		DatabaseURL:              databaseURL,
		SystemDBPool:             systemDBPool,
		DatabaseSchema:           r.config.DatabaseSchema,
		Logger:                   newDBOSLogger(r.app.MakeLog(), r.config.LogLevel),
		ConductorURL:             r.config.ConductorURL,
		ConductorAPIKey:          r.config.ConductorAPIKey,
		ApplicationVersion:       r.config.ApplicationVersion,
		ExecutorID:               r.config.ExecutorID,
		EnablePatching:           r.config.EnablePatching,
		SchedulerPollingInterval: r.config.SchedulerPollingInterval,
		SystemDBStartupTimeout:   r.config.StartupTimeout,
	})
	if err != nil {
		if systemDBPool != nil {
			systemDBPool.Close()
		}
		return nil, fmt.Errorf("goravel-dbos: initialize executor: %w", err)
	}
	return ctx, nil
}

func (r *Runtime) newClient() (dbossdk.Client, error) {
	databaseURL, systemDBPool, err := r.databaseInputs()
	if err != nil {
		return nil, err
	}
	client, err := dbossdk.NewClient(r.app.Context(), dbossdk.ClientConfig{
		AppName:                r.config.AppName,
		DatabaseURL:            databaseURL,
		SystemDBPool:           systemDBPool,
		DatabaseSchema:         r.config.DatabaseSchema,
		Logger:                 newDBOSLogger(r.app.MakeLog(), r.config.LogLevel),
		SystemDBStartupTimeout: r.config.StartupTimeout,
	})
	if err != nil {
		if systemDBPool != nil {
			systemDBPool.Close()
		}
		return nil, err
	}
	return client, nil
}

func (r *Runtime) databaseInputs() (string, *pgxpool.Pool, error) {
	if r.config.DatabaseDriver != "postgres" {
		return r.config.DatabaseURL, nil, nil
	}
	poolConfig, err := pgxpool.ParseConfig(r.config.DatabaseURL)
	if err != nil {
		return "", nil, fmt.Errorf("goravel-dbos: parse native PostgreSQL connection: %w", err)
	}
	poolConfig.MaxConns = r.config.PoolMaxConns
	poolConfig.MinConns = r.config.PoolMinConns
	poolConfig.MaxConnLifetime = r.config.PoolMaxConnLifetime
	poolConfig.MaxConnIdleTime = r.config.PoolMaxConnIdleTime
	poolConfig.ConnConfig.ConnectTimeout = r.config.PoolConnectTimeout
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = r.config.AppName
	pool, err := pgxpool.NewWithConfig(r.app.Context(), poolConfig)
	if err != nil {
		return "", nil, fmt.Errorf("goravel-dbos: create DBOS PostgreSQL pool: %w", err)
	}
	return "", pool, nil
}

func (r *Runtime) closeFailedExecutor(ctx dbossdk.Context) {
	_ = dbossdk.Shutdown(ctx, r.config.ShutdownTimeout)
	r.mu.Lock()
	if r.executor == ctx {
		r.executor = nil
	}
	r.closed = true
	r.mu.Unlock()
}

func (r *Runtime) Executor() (dbossdk.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.executor == nil {
		return nil, ErrNotExecutor
	}
	return r.executor, nil
}

func (r *Runtime) Shutdown() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	executor := r.executor
	client := r.client
	relayCancel := r.relayCancel
	relayDone := r.relayDone
	r.relayCancel = nil
	r.relayDone = nil
	r.executor = nil
	r.client = nil
	r.mu.Unlock()
	if relayCancel != nil {
		relayCancel()
	}
	if relayDone != nil {
		select {
		case <-relayDone:
		case <-time.After(r.config.ShutdownTimeout):
		}
	}

	var errs []error
	if executor != nil {
		if err := dbossdk.Shutdown(executor, r.config.ShutdownTimeout); err != nil {
			errs = append(errs, fmt.Errorf("shutdown executor: %w", err))
		}
	}
	if client != nil && client != executor {
		if err := dbossdk.Shutdown(client, r.config.ShutdownTimeout); err != nil {
			errs = append(errs, fmt.Errorf("shutdown producer client: %w", err))
		}
	}
	return errors.Join(errs...)
}

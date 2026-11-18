package goraveldbos

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/goravel/framework/contracts/database"
	"github.com/jackc/pgx/v5/pgxpool"
)

type configReader interface {
	GetBool(string, ...bool) bool
	GetString(string, ...string) string
	GetInt(string, ...int) int
	GetStringSlice(string, ...[]string) []string
}

const (
	RoleProducer = "producer"
	RoleWorker   = "worker"
	RoleEmbedded = "embedded"
	RoleDisabled = "disabled"
)

var defaultWorkerDisabledRunners = []string{
	"goravel:http",
	"goravel:grpc",
	"goravel:schedule",
	"goravel:queue",
}

// Config is the validated package configuration used by both producer and
// executor processes. Secrets remain in DatabaseURL and ConductorAPIKey and
// must never be rendered by diagnostics.
type Config struct {
	Enabled                  bool
	Role                     string
	AppName                  string
	ApplicationVersion       string
	DatabaseConnection       string
	DatabaseDriver           string
	DatabaseURL              string
	DatabaseSchema           string
	PoolMaxConns             int32
	PoolMinConns             int32
	PoolMaxConnLifetime      time.Duration
	PoolMaxConnIdleTime      time.Duration
	PoolConnectTimeout       time.Duration
	ExecutorID               string
	ConductorURL             string
	ConductorAPIKey          string
	LogLevel                 string
	StartupTimeout           time.Duration
	ShutdownTimeout          time.Duration
	SchedulerPollingInterval time.Duration
	ListenQueues             []string
	// Deprecated: DBOS Go removed its admin server. Enabling it is rejected.
	AdminServer bool
	// Deprecated: retained for source compatibility; no server is started.
	AdminServerPort          int
	EnablePatching           bool
	DisableGoravelRunners    bool
	WorkerDisabledRunners    []string
	SQLiteExperimental       bool
	SQLiteRelayPollInterval  time.Duration
	SQLiteLeaseDuration      time.Duration
	SQLiteRetryBase          time.Duration
	SQLiteRetryMax           time.Duration
	SQLiteDeliveredRetention time.Duration
}

func loadConfig(config configReader, native database.Config) (Config, error) {
	if config == nil {
		return Config{}, errors.New("goravel-dbos: Goravel config service is unavailable")
	}

	cfg := Config{
		Enabled:                  config.GetBool("dbos.enabled", true),
		Role:                     strings.ToLower(strings.TrimSpace(config.GetString("dbos.role", RoleProducer))),
		AppName:                  strings.TrimSpace(config.GetString("dbos.app_name")),
		ApplicationVersion:       strings.TrimSpace(config.GetString("dbos.application_version")),
		DatabaseConnection:       strings.TrimSpace(config.GetString("database.default")),
		DatabaseDriver:           normalizeDatabaseDriver(native.Driver),
		DatabaseSchema:           strings.TrimSpace(config.GetString("dbos.schema", "dbos")),
		PoolMaxConns:             int32(config.GetInt("dbos.pool.max_conns", 20)),
		PoolMinConns:             int32(config.GetInt("dbos.pool.min_conns", 0)),
		PoolMaxConnLifetime:      seconds(config.GetInt("dbos.pool.max_conn_lifetime_seconds", 3600)),
		PoolMaxConnIdleTime:      seconds(config.GetInt("dbos.pool.max_conn_idle_time_seconds", 300)),
		PoolConnectTimeout:       seconds(config.GetInt("dbos.pool.connect_timeout_seconds", 10)),
		ExecutorID:               strings.TrimSpace(config.GetString("dbos.executor_id")),
		ConductorURL:             strings.TrimSpace(config.GetString("dbos.conductor.url")),
		ConductorAPIKey:          strings.TrimSpace(config.GetString("dbos.conductor.api_key")),
		LogLevel:                 strings.ToLower(strings.TrimSpace(config.GetString("dbos.log_level", "info"))),
		StartupTimeout:           seconds(config.GetInt("dbos.startup_timeout_seconds", 120)),
		ShutdownTimeout:          seconds(config.GetInt("dbos.shutdown_timeout_seconds", 30)),
		SchedulerPollingInterval: seconds(config.GetInt("dbos.scheduler_polling_interval_seconds", 30)),
		ListenQueues:             cleanStrings(config.GetStringSlice("dbos.listen_queues", []string{})),
		AdminServer:              config.GetBool("dbos.admin_server.enabled", false),
		AdminServerPort:          config.GetInt("dbos.admin_server.port", 3001),
		EnablePatching:           config.GetBool("dbos.enable_patching", false),
		DisableGoravelRunners:    config.GetBool("dbos.worker.disable_goravel_runners", true),
		WorkerDisabledRunners:    cleanStrings(config.GetStringSlice("dbos.worker.disabled_runners", defaultWorkerDisabledRunners)),
		SQLiteExperimental:       config.GetBool("dbos.sqlite.experimental", false),
		SQLiteRelayPollInterval:  milliseconds(config.GetInt("dbos.sqlite.relay_poll_milliseconds", 500)),
		SQLiteLeaseDuration:      seconds(config.GetInt("dbos.sqlite.lease_seconds", 30)),
		SQLiteRetryBase:          milliseconds(config.GetInt("dbos.sqlite.retry_base_milliseconds", 1000)),
		SQLiteRetryMax:           seconds(config.GetInt("dbos.sqlite.retry_max_seconds", 300)),
		SQLiteDeliveredRetention: seconds(config.GetInt("dbos.sqlite.delivered_retention_seconds", 86400)),
	}
	if !cfg.Enabled {
		cfg.Role = RoleDisabled
	}
	if cfg.AppName == "" {
		cfg.AppName = strings.TrimSpace(config.GetString("app.name"))
	}
	if cfg.ApplicationVersion == "" {
		cfg.ApplicationVersion = strings.TrimSpace(config.GetString("app.version"))
	}
	if override := strings.TrimSpace(os.Getenv("DBOS__APPVERSION")); override != "" {
		if cfg.ApplicationVersion != "" && cfg.ApplicationVersion != override {
			return Config{}, fmt.Errorf("goravel-dbos: DBOS__APPVERSION %q conflicts with dbos.application_version %q", override, cfg.ApplicationVersion)
		}
		cfg.ApplicationVersion = override
	}
	if override := strings.TrimSpace(os.Getenv("DBOS__VMID")); override != "" {
		if cfg.ExecutorID != "" && cfg.ExecutorID != override {
			return Config{}, fmt.Errorf("goravel-dbos: DBOS__VMID %q conflicts with dbos.executor_id %q", override, cfg.ExecutorID)
		}
		cfg.ExecutorID = override
	}

	if err := validateConfig(config, &cfg, native); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateConfig(config configReader, cfg *Config, native database.Config) error {
	if cfg.Role != RoleProducer && cfg.Role != RoleWorker && cfg.Role != RoleEmbedded && cfg.Role != RoleDisabled {
		return fmt.Errorf("goravel-dbos: invalid role %q", cfg.Role)
	}
	if cfg.Role == RoleDisabled {
		return nil
	}
	if cfg.AppName == "" {
		return errors.New("goravel-dbos: dbos.app_name or app.name is required")
	}
	if cfg.AdminServer {
		return errors.New("goravel-dbos: DBOS Go no longer provides an admin server; disable dbos.admin_server.enabled and replace the /dbos-healthz probe")
	}
	if cfg.ApplicationVersion == "" {
		if strings.EqualFold(config.GetString("app.env"), "production") {
			return errors.New("goravel-dbos: dbos.application_version is required in production")
		}
		cfg.ApplicationVersion = "dev"
	}
	if cfg.DatabaseSchema == "" || strings.IndexByte(cfg.DatabaseSchema, 0) >= 0 {
		return errors.New("goravel-dbos: dbos.schema must not be empty or contain NUL")
	}
	if cfg.StartupTimeout <= 0 || cfg.ShutdownTimeout <= 0 || cfg.SchedulerPollingInterval <= 0 ||
		cfg.SQLiteRelayPollInterval <= 0 || cfg.SQLiteLeaseDuration <= 0 || cfg.SQLiteRetryBase <= 0 ||
		cfg.SQLiteRetryMax < cfg.SQLiteRetryBase || cfg.SQLiteDeliveredRetention <= 0 {
		return errors.New("goravel-dbos: lifecycle, scheduler and SQLite relay intervals must be positive, and retry maximum must not be below its base")
	}
	if cfg.LogLevel != "debug" && cfg.LogLevel != "info" && cfg.LogLevel != "warn" && cfg.LogLevel != "error" {
		return fmt.Errorf("goravel-dbos: invalid log level %q", cfg.LogLevel)
	}
	if cfg.PoolMaxConns < 1 || cfg.PoolMinConns < 0 || cfg.PoolMinConns > cfg.PoolMaxConns {
		return errors.New("goravel-dbos: dbos.pool.max_conns must be positive and min_conns must be between zero and max_conns")
	}
	if cfg.PoolMaxConnLifetime <= 0 || cfg.PoolMaxConnIdleTime <= 0 || cfg.PoolConnectTimeout <= 0 {
		return errors.New("goravel-dbos: DBOS pool lifetime, idle time and connect timeout must be positive")
	}

	if cfg.DatabaseConnection == "" {
		return errors.New("goravel-dbos: database.default must select a PostgreSQL or SQLite connection")
	}
	var err error
	switch cfg.DatabaseDriver {
	case "postgres", "postgresql":
		cfg.DatabaseDriver = "postgres"
		cfg.DatabaseURL, err = postgresURLFromGoravel(native)
	case "sqlite", "sqlite3":
		if !cfg.SQLiteExperimental {
			return errors.New("goravel-dbos: SQLite support is experimental and disabled by default; set dbos.sqlite.experimental=true (DBOS_SQLITE_EXPERIMENTAL=true) to opt in")
		}
		cfg.DatabaseDriver = "sqlite"
		cfg.DatabaseURL, err = sqliteURLFromGoravel(native)
	default:
		return fmt.Errorf("goravel-dbos: database.default connection %q uses unsupported driver %q; DBOS supports PostgreSQL and file-backed SQLite", cfg.DatabaseConnection, cfg.DatabaseDriver)
	}
	if err != nil {
		return err
	}
	return nil
}

func sqliteURLFromGoravel(native database.Config) (string, error) {
	dsn := strings.TrimSpace(native.Dsn)
	if dsn == "" {
		dsn = strings.TrimSpace(native.Database)
	}
	if dsn == "" {
		return "", errors.New("goravel-dbos: native SQLite connection must define dsn or database")
	}
	for _, prefix := range []string{"sqlite:", "sqlite3:"} {
		if strings.HasPrefix(strings.ToLower(dsn), prefix) {
			dsn = dsn[len(prefix):]
			break
		}
	}
	if strings.HasPrefix(strings.ToLower(dsn), ":memory:") {
		return "", errors.New("goravel-dbos: in-memory SQLite cannot be shared safely between Goravel and DBOS; use a file-backed database")
	}
	uri, err := url.Parse(dsn)
	if err != nil {
		return "", errors.New("goravel-dbos: invalid native SQLite connection")
	}
	path := uri.Opaque
	if path == "" {
		path = uri.Path
	}
	path, err = url.PathUnescape(path)
	if err != nil {
		return "", errors.New("goravel-dbos: invalid native SQLite connection")
	}
	if strings.EqualFold(path, ":memory:") || strings.EqualFold(uri.Query().Get("mode"), "memory") {
		return "", errors.New("goravel-dbos: in-memory SQLite cannot be shared safely between Goravel and DBOS; use a file-backed database")
	}
	return "sqlite:" + dsn, nil
}

func postgresURLFromGoravel(native database.Config) (string, error) {
	if dsn := strings.TrimSpace(native.Dsn); dsn != "" {
		if _, err := pgxpool.ParseConfig(dsn); err != nil {
			return "", errors.New("goravel-dbos: native PostgreSQL DSN is invalid")
		}
		return dsn, nil
	}

	host := strings.TrimSpace(native.Host)
	port := native.Port
	database := strings.TrimSpace(native.Database)
	username := native.Username
	password := native.Password
	sslmode := native.Sslmode
	timezone := native.Timezone
	searchPath := native.Schema
	if host == "" || database == "" || username == "" {
		return "", errors.New("goravel-dbos: native PostgreSQL connection must define host, database and username")
	}
	if port < 1 || port > 65535 {
		return "", errors.New("goravel-dbos: invalid native PostgreSQL port")
	}
	u := &url.URL{Scheme: "postgresql", Path: "/" + database}
	query := url.Values{}
	if filepath.IsAbs(host) {
		query.Set("host", host)
		query.Set("port", strconv.Itoa(port))
	} else {
		u.Host = net.JoinHostPort(host, strconv.Itoa(port))
	}
	if password == "" {
		u.User = url.User(username)
	} else {
		u.User = url.UserPassword(username, password)
	}
	query.Set("sslmode", sslmode)
	query.Set("timezone", timezone)
	query.Set("search_path", searchPath)
	u.RawQuery = query.Encode()
	dsn := u.String()
	if _, err := pgxpool.ParseConfig(dsn); err != nil {
		return "", errors.New("goravel-dbos: native PostgreSQL connection is invalid")
	}
	return dsn, nil
}

func seconds(value int) time.Duration      { return time.Duration(value) * time.Second }
func milliseconds(value int) time.Duration { return time.Duration(value) * time.Millisecond }

func cleanStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

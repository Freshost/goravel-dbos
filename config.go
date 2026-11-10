package goraveldbos

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

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

var applicationNamePattern = regexp.MustCompile(`^[a-z0-9_-]{3,256}$`)

func loadConfig(config configReader, databaseDriver ...string) (Config, error) {
	if config == nil {
		return Config{}, errors.New("goravel-dbos: Goravel config service is unavailable")
	}

	cfg := Config{
		Enabled:                  config.GetBool("dbos.enabled", true),
		Role:                     strings.ToLower(strings.TrimSpace(config.GetString("dbos.role", RoleProducer))),
		AppName:                  strings.TrimSpace(config.GetString("dbos.app_name")),
		ApplicationVersion:       strings.TrimSpace(config.GetString("dbos.application_version")),
		DatabaseConnection:       strings.TrimSpace(config.GetString("database.default")),
		DatabaseDriver:           strings.ToLower(strings.TrimSpace(firstString(databaseDriver))),
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

	if err := validateConfig(config, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateConfig(config configReader, cfg *Config) error {
	if cfg.Role != RoleProducer && cfg.Role != RoleWorker && cfg.Role != RoleEmbedded && cfg.Role != RoleDisabled {
		return fmt.Errorf("goravel-dbos: invalid role %q", cfg.Role)
	}
	if cfg.Role == RoleDisabled {
		return nil
	}
	if cfg.AppName == "" {
		return errors.New("goravel-dbos: dbos.app_name or app.name is required")
	}
	if !applicationNamePattern.MatchString(cfg.AppName) {
		return errors.New("goravel-dbos: dbos.app_name must contain 3 to 256 lowercase letters, numbers, dashes or underscores")
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
		cfg.DatabaseURL, err = postgresURLFromGoravel(config, cfg.DatabaseConnection)
	case "sqlite", "sqlite3":
		if !cfg.SQLiteExperimental {
			return errors.New("goravel-dbos: SQLite support is experimental and disabled by default; set dbos.sqlite.experimental=true (DBOS_SQLITE_EXPERIMENTAL=true) to opt in")
		}
		cfg.DatabaseDriver = "sqlite"
		cfg.DatabaseURL, err = sqliteURLFromGoravel(config, cfg.DatabaseConnection)
	default:
		return fmt.Errorf("goravel-dbos: database.default connection %q uses unsupported driver %q; DBOS supports PostgreSQL and file-backed SQLite", cfg.DatabaseConnection, cfg.DatabaseDriver)
	}
	if err != nil {
		return err
	}
	return nil
}

func sqliteURLFromGoravel(config configReader, connection string) (string, error) {
	prefix := "database.connections." + connection + "."
	dsn := strings.TrimSpace(config.GetString(prefix + "dsn"))
	if dsn == "" {
		dsn = strings.TrimSpace(config.GetString(prefix + "database"))
	}
	if dsn == "" {
		return "", fmt.Errorf("goravel-dbos: SQLite connection %q must define dsn or database", connection)
	}
	lower := strings.ToLower(dsn)
	if lower == ":memory:" || strings.Contains(lower, "mode=memory") || lower == "file::memory:" {
		return "", errors.New("goravel-dbos: in-memory SQLite cannot be shared safely between Goravel and DBOS; use a file-backed database")
	}
	if strings.HasPrefix(lower, "sqlite:") {
		return dsn, nil
	}
	return "sqlite:" + dsn, nil
}

func postgresURLFromGoravel(config configReader, connection string) (string, error) {
	prefix := "database.connections." + connection + "."
	if dsn := strings.TrimSpace(config.GetString(prefix + "dsn")); dsn != "" {
		if _, err := pgxpool.ParseConfig(dsn); err != nil {
			return "", fmt.Errorf("goravel-dbos: database.default connection %q is not a valid PostgreSQL DSN: %w", connection, err)
		}
		return dsn, nil
	}

	host := strings.TrimSpace(config.GetString(prefix + "host"))
	port := strings.TrimSpace(config.GetString(prefix+"port", "5432"))
	database := strings.TrimSpace(config.GetString(prefix + "database"))
	username := config.GetString(prefix + "username")
	password := config.GetString(prefix + "password")
	sslmode := strings.TrimSpace(config.GetString(prefix+"sslmode", "disable"))
	timezone := strings.TrimSpace(config.GetString(prefix+"timezone", config.GetString("app.timezone", "UTC")))
	searchPath := strings.TrimSpace(config.GetString(prefix+"schema", "public"))
	if host == "" || database == "" || username == "" {
		return "", fmt.Errorf("goravel-dbos: database.default connection %q must be PostgreSQL and define host, database and username", connection)
	}
	u := &url.URL{Scheme: "postgresql", Path: "/" + database}
	query := url.Values{}
	if filepath.IsAbs(host) {
		query.Set("host", host)
	} else {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return "", fmt.Errorf("goravel-dbos: invalid PostgreSQL port %q", port)
		}
		u.Host = net.JoinHostPort(host, port)
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
	return u.String(), nil
}

func seconds(value int) time.Duration      { return time.Duration(value) * time.Second }
func milliseconds(value int) time.Duration { return time.Duration(value) * time.Millisecond }

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

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

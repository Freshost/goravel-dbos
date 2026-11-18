package goraveldbos

import (
	"fmt"
	"os"
	"testing"

	"github.com/goravel/framework/contracts/database"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type fakeConfig map[string]any

func loadTestConfig(input fakeConfig, drivers ...string) (Config, error) {
	driver := ""
	if len(drivers) > 0 {
		driver = drivers[0]
	}
	prefix := "database.connections." + input.GetString("database.default") + "."
	return loadConfig(input, database.Config{
		Driver:   driver,
		Dsn:      input.GetString(prefix + "dsn"),
		Host:     input.GetString(prefix + "host"),
		Port:     input.GetInt(prefix+"port", 5432),
		Database: input.GetString(prefix + "database"),
		Username: input.GetString(prefix + "username"),
		Password: input.GetString(prefix + "password"),
		Sslmode:  input.GetString(prefix+"sslmode", "disable"),
		Timezone: input.GetString(prefix+"timezone", input.GetString("app.timezone", "UTC")),
		Schema:   input.GetString(prefix+"schema", "public"),
	})
}

func (f fakeConfig) GetBool(key string, defaults ...bool) bool {
	if value, exists := f[key]; exists {
		return value.(bool)
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return false
}

func (f fakeConfig) GetString(key string, defaults ...string) string {
	if value, exists := f[key]; exists {
		return fmt.Sprint(value)
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return ""
}

func (f fakeConfig) GetInt(key string, defaults ...int) int {
	if value, exists := f[key]; exists {
		var result int
		_, _ = fmt.Sscan(fmt.Sprint(value), &result)
		return result
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return 0
}

func (f fakeConfig) GetStringSlice(key string, defaults ...[]string) []string {
	if value, exists := f[key]; exists {
		return value.([]string)
	}
	if len(defaults) > 0 {
		return defaults[0]
	}
	return nil
}

func validConfig() fakeConfig {
	return fakeConfig{
		"dbos.enabled":                          true,
		"dbos.role":                             RoleProducer,
		"dbos.app_name":                         "billing",
		"dbos.application_version":              "2026.08.1",
		"database.default":                      "primary",
		"database.connections.primary.host":     "db.internal",
		"database.connections.primary.port":     "5433",
		"database.connections.primary.database": "billing data",
		"database.connections.primary.username": "app user",
		"database.connections.primary.password": "p@ss/word",
		"database.connections.primary.sslmode":  "require",
		"database.connections.primary.schema":   "tenant",
		"app.timezone":                          "Europe/Prague",
		"dbos.worker.disable_goravel_runners":   true,
		"dbos.worker.disabled_runners":          []string{"goravel:http"},
	}
}

func TestLoadConfigBuildsEscapedNativePostgresURL(t *testing.T) {
	cfg, err := loadTestConfig(validConfig(), "postgres")
	require.NoError(t, err)
	require.Equal(t, RoleProducer, cfg.Role)
	require.Equal(t, "primary", cfg.DatabaseConnection)
	require.Equal(t, "postgresql://app%20user:p%40ss%2Fword@db.internal:5433/billing%20data?search_path=tenant&sslmode=require&timezone=Europe%2FPrague", cfg.DatabaseURL)
	require.Equal(t, int32(20), cfg.PoolMaxConns)
	require.Equal(t, int32(0), cfg.PoolMinConns)
	require.True(t, cfg.DisableGoravelRunners)
}

func TestLoadConfigBuildsNativePostgresUnixSocketURL(t *testing.T) {
	input := validConfig()
	input["database.connections.primary.host"] = "/run/postgresql"
	input["database.connections.primary.port"] = "5433"

	cfg, err := loadTestConfig(input, "postgres")
	require.NoError(t, err)
	require.Equal(t, "postgresql://app%20user:p%40ss%2Fword@/billing%20data?host=%2Frun%2Fpostgresql&port=5433&search_path=tenant&sslmode=require&timezone=Europe%2FPrague", cfg.DatabaseURL)
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	require.NoError(t, err)
	require.Equal(t, "/run/postgresql", poolConfig.ConnConfig.Host)
	require.Equal(t, uint16(5433), poolConfig.ConnConfig.Port)
}

func TestLoadConfigValidatesDBOSPoolBudget(t *testing.T) {
	input := validConfig()
	input["dbos.pool.max_conns"] = 5
	input["dbos.pool.min_conns"] = 6

	_, err := loadTestConfig(input, "postgres")
	require.ErrorContains(t, err, "min_conns")
}

func TestLoadConfigRequiresVersionInProduction(t *testing.T) {
	input := validConfig()
	input["dbos.application_version"] = ""
	input["app.env"] = "production"
	_, err := loadTestConfig(input, "postgres")
	require.ErrorContains(t, err, "application_version is required")
}

func TestLoadConfigRejectsRemovedAdminServer(t *testing.T) {
	input := validConfig()
	input["dbos.admin_server.enabled"] = true
	_, err := loadTestConfig(input, "postgres")
	require.ErrorContains(t, err, "no longer provides an admin server")
}

func TestLoadConfigRejectsConflictingDBOSOverride(t *testing.T) {
	previous, existed := os.LookupEnv("DBOS__APPVERSION")
	require.NoError(t, os.Setenv("DBOS__APPVERSION", "other"))
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv("DBOS__APPVERSION", previous)
		} else {
			_ = os.Unsetenv("DBOS__APPVERSION")
		}
	})
	_, err := loadTestConfig(validConfig(), "postgres")
	require.ErrorContains(t, err, "conflicts")
}

func TestLoadConfigRejectsUnsupportedNativeDefaultConnection(t *testing.T) {
	input := validConfig()
	input["database.default"] = "sqlite"
	_, err := loadTestConfig(input, "mysql")
	require.ErrorContains(t, err, "unsupported driver")
}

func TestLoadConfigReusesNativePostgresDSN(t *testing.T) {
	input := validConfig()
	input["database.connections.primary.dsn"] = "postgresql://native:native@db.internal:5432/native?sslmode=require"
	cfg, err := loadTestConfig(input, "postgres")
	require.NoError(t, err)
	require.Equal(t, input["database.connections.primary.dsn"], cfg.DatabaseURL)
}

func TestLoadConfigReusesFileBackedSQLite(t *testing.T) {
	input := validConfig()
	input["database.default"] = "sqlite"
	input["database.connections.sqlite.database"] = "/tmp/app.db"
	input["dbos.sqlite.experimental"] = true
	cfg, err := loadTestConfig(input, "sqlite")
	require.NoError(t, err)
	require.Equal(t, "sqlite", cfg.DatabaseDriver)
	require.Equal(t, "sqlite:/tmp/app.db", cfg.DatabaseURL)
	require.True(t, cfg.SQLiteExperimental)
}

func TestLoadConfigRequiresExplicitExperimentalSQLiteOptIn(t *testing.T) {
	input := validConfig()
	input["database.default"] = "sqlite"
	input["database.connections.sqlite.database"] = "/tmp/app.db"
	_, err := loadTestConfig(input, "sqlite")
	require.ErrorContains(t, err, "SQLite support is experimental")
	require.ErrorContains(t, err, "DBOS_SQLITE_EXPERIMENTAL=true")
}

func TestLoadConfigRejectsInMemorySQLite(t *testing.T) {
	for _, dsn := range []string{
		":memory:", "file::memory:", "file::memory:?cache=shared",
		"sqlite::memory:", "sqlite:file::memory:?cache=shared",
		"sqlite3:file::memory:?cache=shared", "file:app?mode=memory",
		"file:app?mode=%6demory", "file:%3Amemory%3A?cache=shared",
	} {
		t.Run(dsn, func(t *testing.T) {
			input := validConfig()
			input["database.default"] = "sqlite"
			input["database.connections.sqlite.dsn"] = dsn
			input["dbos.sqlite.experimental"] = true
			_, err := loadTestConfig(input, "sqlite")
			require.ErrorContains(t, err, "in-memory SQLite")
		})
	}
}

func TestInvalidPostgresDSNDoesNotExposeCredentials(t *testing.T) {
	input := validConfig()
	input["database.connections.primary.dsn"] = "postgresql://app@localhost/billing?password=private&sslmode=invalid"
	_, err := loadTestConfig(input, "postgres")
	require.ErrorContains(t, err, "DSN is invalid")
	require.NotContains(t, err.Error(), "private")
	require.NotContains(t, err.Error(), "postgresql://")
}

func TestDisabledRoleDoesNotRequireDatabaseConfiguration(t *testing.T) {
	cfg, err := loadTestConfig(fakeConfig{
		"dbos.enabled": false,
		"dbos.role":    RoleProducer,
	})
	require.NoError(t, err)
	require.Equal(t, RoleDisabled, cfg.Role)
}

package goraveldbos

import (
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type fakeConfig map[string]any

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
	cfg, err := loadConfig(validConfig(), "postgres")
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
	input["database.connections.primary.port"] = "not-used-for-sockets"

	cfg, err := loadConfig(input, "postgres")
	require.NoError(t, err)
	require.Equal(t, "postgresql://app%20user:p%40ss%2Fword@/billing%20data?host=%2Frun%2Fpostgresql&search_path=tenant&sslmode=require&timezone=Europe%2FPrague", cfg.DatabaseURL)
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	require.NoError(t, err)
	require.Equal(t, "/run/postgresql", poolConfig.ConnConfig.Host)
	require.Equal(t, uint16(5432), poolConfig.ConnConfig.Port)
}

func TestLoadConfigValidatesDBOSPoolBudget(t *testing.T) {
	input := validConfig()
	input["dbos.pool.max_conns"] = 5
	input["dbos.pool.min_conns"] = 6

	_, err := loadConfig(input, "postgres")
	require.ErrorContains(t, err, "min_conns")
}

func TestLoadConfigRequiresVersionInProduction(t *testing.T) {
	input := validConfig()
	input["dbos.application_version"] = ""
	input["app.env"] = "production"
	_, err := loadConfig(input, "postgres")
	require.ErrorContains(t, err, "application_version is required")
}

func TestLoadConfigValidatesApplicationName(t *testing.T) {
	for _, name := range []string{"FreshAgents", "ab", "fresh.agents"} {
		t.Run(name, func(t *testing.T) {
			input := validConfig()
			input["dbos.app_name"] = name
			_, err := loadConfig(input, "postgres")
			require.ErrorContains(t, err, "dbos.app_name must contain")
		})
	}
}

func TestLoadConfigRejectsRemovedAdminServer(t *testing.T) {
	input := validConfig()
	input["dbos.admin_server.enabled"] = true
	_, err := loadConfig(input, "postgres")
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
	_, err := loadConfig(validConfig(), "postgres")
	require.ErrorContains(t, err, "conflicts")
}

func TestLoadConfigRejectsUnsupportedNativeDefaultConnection(t *testing.T) {
	input := validConfig()
	input["database.default"] = "sqlite"
	_, err := loadConfig(input, "mysql")
	require.ErrorContains(t, err, "unsupported driver")
}

func TestLoadConfigReusesNativePostgresDSN(t *testing.T) {
	input := validConfig()
	input["database.connections.primary.dsn"] = "postgresql://native:native@db.internal:5432/native?sslmode=require"
	cfg, err := loadConfig(input, "postgres")
	require.NoError(t, err)
	require.Equal(t, input["database.connections.primary.dsn"], cfg.DatabaseURL)
}

func TestLoadConfigReusesFileBackedSQLite(t *testing.T) {
	input := validConfig()
	input["database.default"] = "sqlite"
	input["database.connections.sqlite.database"] = "/tmp/app.db"
	input["dbos.sqlite.experimental"] = true
	cfg, err := loadConfig(input, "sqlite")
	require.NoError(t, err)
	require.Equal(t, "sqlite", cfg.DatabaseDriver)
	require.Equal(t, "sqlite:/tmp/app.db", cfg.DatabaseURL)
	require.True(t, cfg.SQLiteExperimental)
}

func TestLoadConfigRequiresExplicitExperimentalSQLiteOptIn(t *testing.T) {
	input := validConfig()
	input["database.default"] = "sqlite"
	input["database.connections.sqlite.database"] = "/tmp/app.db"
	_, err := loadConfig(input, "sqlite")
	require.ErrorContains(t, err, "SQLite support is experimental")
	require.ErrorContains(t, err, "DBOS_SQLITE_EXPERIMENTAL=true")
}

func TestLoadConfigRejectsInMemorySQLite(t *testing.T) {
	input := validConfig()
	input["database.default"] = "sqlite"
	input["database.connections.sqlite.dsn"] = ":memory:"
	input["dbos.sqlite.experimental"] = true
	_, err := loadConfig(input, "sqlite")
	require.ErrorContains(t, err, "in-memory SQLite")
}

func TestDisabledRoleDoesNotRequireDatabaseConfiguration(t *testing.T) {
	cfg, err := loadConfig(fakeConfig{
		"dbos.enabled": false,
		"dbos.role":    RoleProducer,
	})
	require.NoError(t, err)
	require.Equal(t, RoleDisabled, cfg.Role)
}

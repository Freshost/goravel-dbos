// This file is published into a consuming Goravel application's config
// directory. All settings are optional outside production; production requires
// a stable application name and version.
package config

import "github.com/goravel/framework/facades"

func init() {
	config := facades.Config()
	config.Add("dbos", map[string]any{
		"enabled":             config.Env("DBOS_ENABLED", true),
		"role":                config.Env("DBOS_ROLE", "producer"),
		"app_name":            config.Env("DBOS_APP_NAME", ""),
		"application_version": config.Env("DBOS_APP_VERSION", config.Env("APP_VERSION", "")),
		// DBOS always reuses database.default and its native Goravel connection
		// settings. PostgreSQL is production-supported; SQLite is experimental.
		// There is intentionally no separate DBOS database.
		"schema":                             config.Env("DBOS_SCHEMA", "dbos"),
		"executor_id":                        config.Env("DBOS_EXECUTOR_ID", ""),
		"log_level":                          config.Env("DBOS_LOG_LEVEL", "info"),
		"startup_timeout_seconds":            config.Env("DBOS_STARTUP_TIMEOUT_SECONDS", 120),
		"shutdown_timeout_seconds":           config.Env("DBOS_SHUTDOWN_TIMEOUT_SECONDS", 30),
		"scheduler_polling_interval_seconds": config.Env("DBOS_SCHEDULER_POLLING_INTERVAL_SECONDS", 30),
		"listen_queues":                      []string{},
		"enable_patching":                    config.Env("DBOS_ENABLE_PATCHING", false),
		"sqlite": map[string]any{
			// SQLite is an explicit experimental opt-in. PostgreSQL is the only
			// fully supported production database for goravel-dbos.
			"experimental":                config.Env("DBOS_SQLITE_EXPERIMENTAL", false),
			"relay_poll_milliseconds":     config.Env("DBOS_SQLITE_RELAY_POLL_MILLISECONDS", 500),
			"lease_seconds":               config.Env("DBOS_SQLITE_LEASE_SECONDS", 30),
			"retry_base_milliseconds":     config.Env("DBOS_SQLITE_RETRY_BASE_MILLISECONDS", 1000),
			"retry_max_seconds":           config.Env("DBOS_SQLITE_RETRY_MAX_SECONDS", 300),
			"delivered_retention_seconds": config.Env("DBOS_SQLITE_DELIVERED_RETENTION_SECONDS", 86400),
		},
		"conductor": map[string]any{
			"url":     config.Env("DBOS_CONDUCTOR_URL", ""),
			"api_key": config.Env("DBOS_CONDUCTOR_API_KEY", ""),
		},
		"admin_server": map[string]any{
			"enabled": config.Env("DBOS_ADMIN_SERVER_ENABLED", false),
			"port":    config.Env("DBOS_ADMIN_SERVER_PORT", 3001),
		},
		"worker": map[string]any{
			// A worker process should execute DBOS and required application
			// dependency runners, not expose HTTP or start a second scheduler.
			"disable_goravel_runners": true,
			"disabled_runners": []string{
				"goravel:http",
				"goravel:grpc",
				"goravel:schedule",
				"goravel:queue",
			},
		},
	})
}

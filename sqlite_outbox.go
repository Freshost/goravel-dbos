package goraveldbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/goravel/framework/contracts/database/orm"
)

const sqliteMigrationsTableSQL = `CREATE TABLE IF NOT EXISTS goravel_dbos_package_migrations (
		version INTEGER PRIMARY KEY,
		applied_at_ms INTEGER NOT NULL
	)`

type sqliteMigration struct {
	version    int
	statements []string
}

var sqliteOutboxMigrations = []sqliteMigration{{
	version: 1,
	statements: []string{
		`CREATE TABLE goravel_dbos_outbox (
		workflow_id TEXT PRIMARY KEY,
		workflow_name TEXT NOT NULL,
		queue_name TEXT NOT NULL,
		input_json TEXT NOT NULL,
		options_json TEXT NOT NULL,
		application_version TEXT NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		available_at_ms INTEGER NOT NULL,
		locked_until_ms INTEGER,
		locked_by TEXT,
		last_error TEXT,
		created_at_ms INTEGER NOT NULL,
		delivered_at_ms INTEGER
	)`,
		`CREATE INDEX goravel_dbos_outbox_pending_idx
		ON goravel_dbos_outbox(delivered_at_ms, available_at_ms, locked_until_ms, created_at_ms)`,
	},
}}

type sqliteDispatchOptions struct {
	TimeoutMilliseconds    int64    `json:"timeout_ms,omitempty"`
	DeduplicationID        string   `json:"deduplication_id,omitempty"`
	Priority               uint     `json:"priority,omitempty"`
	QueuePartitionKey      string   `json:"queue_partition_key,omitempty"`
	ClassName              string   `json:"class_name,omitempty"`
	ConfigName             *string  `json:"config_name,omitempty"`
	AuthenticatedUser      string   `json:"authenticated_user,omitempty"`
	AuthenticatedRoles     []string `json:"authenticated_roles,omitempty"`
	DelayUntilMilliseconds int64    `json:"delay_until_ms,omitempty"`
}

type sqliteOutboxRow struct {
	WorkflowID         string `gorm:"column:workflow_id"`
	WorkflowName       string `gorm:"column:workflow_name"`
	QueueName          string `gorm:"column:queue_name"`
	InputJSON          string `gorm:"column:input_json"`
	OptionsJSON        string `gorm:"column:options_json"`
	ApplicationVersion string `gorm:"column:application_version"`
	Attempts           int    `gorm:"column:attempts"`
}

// SQLiteDispatchStats exposes operational health without exposing payloads.
// It is zero-valued for PostgreSQL because that path writes directly to DBOS.
func (r *Runtime) SQLiteDispatchStats() (pending int64, failed int64, oldestCreatedAtMS int64, err error) {
	if r.config.DatabaseDriver != "sqlite" {
		return 0, 0, 0, nil
	}
	var stats struct {
		Pending           int64 `gorm:"column:pending"`
		Failed            int64 `gorm:"column:failed"`
		OldestCreatedAtMS int64 `gorm:"column:oldest_created_at_ms"`
	}
	err = r.app.MakeOrm().Connection(r.config.DatabaseConnection).Query().Raw(`SELECT
		COUNT(*) AS pending,
		COALESCE(SUM(CASE WHEN last_error IS NOT NULL THEN 1 ELSE 0 END), 0) AS failed,
		COALESCE(MIN(created_at_ms), 0) AS oldest_created_at_ms
		FROM goravel_dbos_outbox WHERE delivered_at_ms IS NULL`).Scan(&stats)
	return stats.Pending, stats.Failed, stats.OldestCreatedAtMS, err
}

func (r *Runtime) migrateSQLiteOutbox() error {
	database := r.app.MakeOrm().Connection(r.config.DatabaseConnection)
	if database == nil {
		return errors.New("goravel-dbos: SQLite ORM connection is unavailable")
	}
	return database.Transaction(func(tx orm.Query) error {
		if _, err := tx.Exec(sqliteMigrationsTableSQL); err != nil {
			return fmt.Errorf("goravel-dbos: initialize internal SQLite migrations: %w", err)
		}
		var state struct {
			Version int `gorm:"column:version"`
		}
		if err := tx.Raw(`SELECT COALESCE(MAX(version), 0) AS version FROM goravel_dbos_package_migrations`).Scan(&state); err != nil {
			return fmt.Errorf("goravel-dbos: read internal SQLite migration version: %w", err)
		}
		for _, migration := range sqliteOutboxMigrations {
			if migration.version <= state.Version {
				continue
			}
			for _, statement := range migration.statements {
				if _, err := tx.Exec(statement); err != nil {
					return fmt.Errorf("goravel-dbos: apply internal SQLite migration %d: %w", migration.version, err)
				}
			}
			if _, err := tx.Exec(`INSERT INTO goravel_dbos_package_migrations(version, applied_at_ms) VALUES (?, ?)`, migration.version, time.Now().UnixMilli()); err != nil {
				return fmt.Errorf("goravel-dbos: record internal SQLite migration %d: %w", migration.version, err)
			}
		}
		return nil
	})
}

func (r *Runtime) startSQLiteRelay() error {
	if r.config.DatabaseDriver != "sqlite" || r.config.Role == RoleDisabled {
		return nil
	}
	client, err := r.Client()
	if err != nil {
		return err
	}
	r.mu.Lock()
	if r.relayDone != nil {
		r.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(r.app.Context())
	done := make(chan struct{})
	r.relayCancel = cancel
	r.relayDone = done
	r.mu.Unlock()
	go r.runSQLiteRelay(ctx, done, client)
	return nil
}

func (r *Runtime) runSQLiteRelay(ctx context.Context, done chan<- struct{}, client dbossdk.Client) {
	defer close(done)
	workerID := uuid.NewString()
	ticker := time.NewTicker(r.config.SQLiteRelayPollInterval)
	defer ticker.Stop()
	for {
		delivered, err := r.relaySQLiteOnce(ctx, client, workerID)
		if err != nil {
			newDBOSLogger(r.app.MakeLog(), r.config.LogLevel).WarnContext(ctx, "SQLite outbox relay failed", "error", err)
		}
		if delivered {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runtime) relaySQLiteOnce(ctx context.Context, client dbossdk.Client, workerID string) (bool, error) {
	now := time.Now()
	query := r.app.MakeOrm().Connection(r.config.DatabaseConnection).WithContext(ctx).Query()
	var row sqliteOutboxRow
	err := query.Raw(`UPDATE goravel_dbos_outbox
		SET locked_by = ?, locked_until_ms = ?, attempts = attempts + 1
		WHERE workflow_id = (
			SELECT workflow_id FROM goravel_dbos_outbox
			WHERE delivered_at_ms IS NULL AND available_at_ms <= ?
			AND (locked_until_ms IS NULL OR locked_until_ms <= ?)
			ORDER BY created_at_ms LIMIT 1
		)
		AND delivered_at_ms IS NULL AND (locked_until_ms IS NULL OR locked_until_ms <= ?)
		RETURNING workflow_id, workflow_name, queue_name, input_json, options_json, application_version, attempts`,
		workerID, now.Add(r.config.SQLiteLeaseDuration).UnixMilli(), now.UnixMilli(), now.UnixMilli(), now.UnixMilli()).Scan(&row)
	if err != nil {
		return false, err
	}
	if row.WorkflowID == "" {
		_, _ = query.Exec(`DELETE FROM goravel_dbos_outbox WHERE delivered_at_ms IS NOT NULL AND delivered_at_ms < ?`, now.Add(-r.config.SQLiteDeliveredRetention).UnixMilli())
		return false, nil
	}

	err = deliverSQLiteOutboxRow(client, row, now)
	if err == nil {
		_, ackErr := query.Exec(`UPDATE goravel_dbos_outbox SET delivered_at_ms = ?, locked_by = NULL, locked_until_ms = NULL, last_error = NULL WHERE workflow_id = ? AND locked_by = ?`, now.UnixMilli(), row.WorkflowID, workerID)
		return true, ackErr
	}
	message := err.Error()
	if len(message) > 2048 {
		message = message[:2048]
	}
	retryAt := now.Add(sqliteRetryDelay(row.Attempts, r.config.SQLiteRetryBase, r.config.SQLiteRetryMax))
	_, releaseErr := query.Exec(`UPDATE goravel_dbos_outbox SET available_at_ms = ?, locked_by = NULL, locked_until_ms = NULL, last_error = ? WHERE workflow_id = ? AND locked_by = ?`, retryAt.UnixMilli(), message, row.WorkflowID, workerID)
	return true, errors.Join(err, releaseErr)
}

func deliverSQLiteOutboxRow(client dbossdk.Client, row sqliteOutboxRow, now time.Time) error {
	var options sqliteDispatchOptions
	if err := json.Unmarshal([]byte(row.OptionsJSON), &options); err != nil {
		return fmt.Errorf("decode dispatch options for workflow %q: %w", row.WorkflowID, err)
	}
	dbosOptions := []dbossdk.EnqueueOption{
		dbossdk.WithEnqueueWorkflowID(row.WorkflowID),
		dbossdk.WithEnqueueApplicationVersion(row.ApplicationVersion),
	}
	if options.TimeoutMilliseconds > 0 {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueueTimeout(time.Duration(options.TimeoutMilliseconds)*time.Millisecond))
	}
	if options.DeduplicationID != "" {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueueDeduplicationID(options.DeduplicationID))
	}
	if options.Priority > 0 {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueuePriority(options.Priority))
	}
	if options.QueuePartitionKey != "" {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueueQueuePartitionKey(options.QueuePartitionKey))
	}
	if options.ClassName != "" {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueueClassName(options.ClassName))
	}
	if options.ConfigName != nil {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueueConfigName(*options.ConfigName))
	}
	if options.AuthenticatedUser != "" {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueueAuthenticatedUser(options.AuthenticatedUser))
	}
	if len(options.AuthenticatedRoles) > 0 {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueueAuthenticatedRoles(options.AuthenticatedRoles...))
	}
	if options.DelayUntilMilliseconds > now.UnixMilli() {
		dbosOptions = append(dbosOptions, dbossdk.WithEnqueueDelay(time.Duration(options.DelayUntilMilliseconds-now.UnixMilli())*time.Millisecond))
	}
	_, err := dbossdk.Enqueue[any](client, row.QueueName, row.WorkflowName, json.RawMessage(row.InputJSON), dbosOptions...)
	if err != nil {
		return fmt.Errorf("enqueue SQLite outbox workflow %q: %w", row.WorkflowID, err)
	}
	return nil
}

func sqliteRetryDelay(attempt int, base, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	jitterLimit := delay / 4
	if jitterLimit > 0 && delay < maximum {
		delay += time.Duration(rand.Int64N(int64(jitterLimit) + 1))
		if delay > maximum {
			delay = maximum
		}
	}
	return delay
}

func encodeSQLiteDispatchOptions(options transactionEnqueueOptions) (string, error) {
	var delayUntilMilliseconds int64
	if options.delay > 0 {
		delayUntilMilliseconds = time.Now().Add(options.delay).UnixMilli()
	}
	encoded, err := json.Marshal(sqliteDispatchOptions{
		TimeoutMilliseconds:    options.timeout.Milliseconds(),
		DeduplicationID:        options.deduplicationID,
		Priority:               options.priority,
		QueuePartitionKey:      options.queuePartitionKey,
		ClassName:              options.className,
		ConfigName:             options.configName,
		AuthenticatedUser:      options.authenticatedUser,
		AuthenticatedRoles:     options.authenticatedRoles,
		DelayUntilMilliseconds: delayUntilMilliseconds,
	})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

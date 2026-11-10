package goraveldbos

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/goravel/framework/contracts/database/orm"
	"github.com/jackc/pgx/v5"

	packagecontracts "github.com/freshost/goravel-dbos/contracts"
)

type transactionEnqueueOptions struct {
	workflowID         string
	timeout            time.Duration
	deadline           time.Time
	deduplicationID    string
	priority           uint
	queuePartitionKey  string
	className          string
	configName         *string
	authenticatedUser  string
	authenticatedRoles []string
	delay              time.Duration
}

// TransactionEnqueueOption configures transaction-bound enqueue. It contains
// the portable intersection supported by PostgreSQL direct enqueue and the
// package's SQLite relay.
type TransactionEnqueueOption func(*transactionEnqueueOptions)

type transactionScanner interface {
	InTransaction() bool
	Driver() string
	ScanRaw(query string, destination any, values ...any) error
	ExecRaw(query string, values ...any) (int64, error)
}

type goravelTransactionScanner struct{ query orm.Query }

func (s goravelTransactionScanner) InTransaction() bool { return s.query.InTransaction() }
func (s goravelTransactionScanner) Driver() string      { return s.query.Driver() }
func (s goravelTransactionScanner) ScanRaw(query string, destination any, values ...any) error {
	return s.query.Raw(query, values...).Scan(destination)
}
func (s goravelTransactionScanner) ExecRaw(query string, values ...any) (int64, error) {
	result, err := s.query.Exec(query, values...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected, nil
}

func WithTransactionWorkflowID(id string) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.workflowID = id }
}

func WithTransactionTimeout(timeout time.Duration) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.timeout = timeout }
}

func WithTransactionDeadline(deadline time.Time) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.deadline = deadline }
}

func WithTransactionDeduplicationID(id string) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.deduplicationID = id }
}

func WithTransactionPriority(priority uint) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.priority = priority }
}

func WithTransactionQueuePartitionKey(key string) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.queuePartitionKey = key }
}

func WithTransactionClassName(name string) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.className = name }
}

func WithTransactionConfigName(name string) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.configName = &name }
}

func WithTransactionAuthenticatedUser(user string) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.authenticatedUser = user }
}

func WithTransactionAuthenticatedRoles(roles ...string) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.authenticatedRoles = append([]string{}, roles...) }
}

func WithTransactionDelay(delay time.Duration) TransactionEnqueueOption {
	return func(options *transactionEnqueueOptions) { options.delay = delay }
}

// EnqueueInTransaction atomically records a DBOS workflow through an active
// Goravel transaction. PostgreSQL writes directly into DBOS. Experimental
// SQLite support records an internal package-owned dispatch that is relayed
// idempotently after commit.
func EnqueueInTransaction[Input any, Output any](
	runtime packagecontracts.Runtime,
	tx orm.Query,
	definition Workflow[Input, Output],
	input Input,
	optionFns ...TransactionEnqueueOption,
) (string, error) {
	if tx == nil {
		return "", errors.New("goravel-dbos: transactional enqueue requires an active Goravel transaction")
	}
	return enqueueWithTransactionScanner(runtime, goravelTransactionScanner{query: tx}, definition, input, optionFns...)
}

func enqueueWithTransactionScanner[Input any, Output any](
	runtime packagecontracts.Runtime,
	tx transactionScanner,
	definition Workflow[Input, Output],
	input Input,
	optionFns ...TransactionEnqueueOption,
) (string, error) {
	if runtime == nil {
		return "", errors.New("goravel-dbos: runtime is required")
	}
	if runtime.Role() == RoleDisabled {
		return "", ErrDisabled
	}
	if tx == nil || !tx.InTransaction() {
		return "", errors.New("goravel-dbos: transactional enqueue requires an active Goravel transaction")
	}
	driver := strings.ToLower(tx.Driver())
	if driver != "postgres" && driver != "postgresql" && driver != "sqlite" && driver != "sqlite3" {
		return "", fmt.Errorf("goravel-dbos: transactional enqueue requires PostgreSQL or SQLite, got %q", tx.Driver())
	}
	if runtime.DatabaseDriver() != "" && normalizeDatabaseDriver(driver) != normalizeDatabaseDriver(runtime.DatabaseDriver()) {
		return "", fmt.Errorf("goravel-dbos: transaction driver %q does not match configured DBOS driver %q", tx.Driver(), runtime.DatabaseDriver())
	}
	if definition.name == "" || definition.queue == "" {
		return "", errors.New("goravel-dbos: invalid zero-value workflow definition")
	}

	options := transactionEnqueueOptions{}
	for _, option := range optionFns {
		if option != nil {
			option(&options)
		}
	}
	if options.workflowID == "" {
		options.workflowID = uuid.NewString()
	}
	if options.timeout < 0 || options.delay < 0 {
		return "", errors.New("goravel-dbos: transaction timeout and delay cannot be negative")
	}
	if options.priority > math.MaxInt32 {
		return "", fmt.Errorf("goravel-dbos: priority %d exceeds the portable DBOS transaction range", options.priority)
	}
	if options.deduplicationID != "" && options.queuePartitionKey != "" {
		return "", errors.New("goravel-dbos: deduplication ID and queue partition key cannot be combined")
	}

	encodedInput, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("goravel-dbos: encode workflow %q input: %w", definition.name, err)
	}
	roles, err := optionalJSON(options.authenticatedRoles)
	if err != nil {
		return "", fmt.Errorf("goravel-dbos: encode authenticated roles: %w", err)
	}

	if normalizeDatabaseDriver(driver) == "sqlite" {
		return enqueueSQLiteDispatch(runtime, tx, definition.name, definition.queue, string(encodedInput), options)
	}
	application, ok := runtime.(packagecontracts.ApplicationRuntime)
	if !ok || strings.TrimSpace(application.ApplicationName()) == "" {
		return "", errors.New("goravel-dbos: PostgreSQL transactional enqueue requires a runtime with ApplicationName")
	}

	schema := pgx.Identifier{runtime.DatabaseSchema()}.Sanitize()
	query := fmt.Sprintf(`SELECT %s.enqueue_workflow(
		?, ?, ARRAY[?::json], '{}'::json,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
	) AS workflow_id`, schema)
	var result struct {
		WorkflowID string `gorm:"column:workflow_id"`
	}
	if err := tx.ScanRaw(query, &result,
		definition.name,
		definition.queue,
		string(encodedInput),
		nullString(options.className),
		options.configName,
		options.workflowID,
		runtime.ApplicationVersion(),
		durationMilliseconds(options.timeout),
		timeMilliseconds(options.deadline),
		nullString(options.deduplicationID),
		optionalPriority(options.priority),
		nullString(options.queuePartitionKey),
		nullString(options.authenticatedUser),
		roles,
		delayUntilMilliseconds(options.delay),
		application.ApplicationName(),
	); err != nil {
		return "", fmt.Errorf("goravel-dbos: transactionally enqueue workflow %q: %w", definition.name, err)
	}
	if result.WorkflowID == "" {
		return "", fmt.Errorf("goravel-dbos: transactional enqueue for %q returned an empty workflow ID", definition.name)
	}
	return result.WorkflowID, nil
}

func enqueueSQLiteDispatch(
	runtime packagecontracts.Runtime,
	tx transactionScanner,
	workflowName string,
	queueName string,
	encodedInput string,
	options transactionEnqueueOptions,
) (string, error) {
	if !options.deadline.IsZero() {
		return "", errors.New("goravel-dbos: transaction deadline is not supported by DBOS SQLite; use a timeout")
	}
	encodedOptions, err := encodeSQLiteDispatchOptions(options)
	if err != nil {
		return "", fmt.Errorf("goravel-dbos: encode SQLite dispatch options: %w", err)
	}
	now := time.Now().UnixMilli()
	_, err = tx.ExecRaw(`INSERT OR IGNORE INTO goravel_dbos_outbox (
		workflow_id, workflow_name, queue_name, input_json, options_json,
		application_version, available_at_ms, created_at_ms
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		options.workflowID, workflowName, queueName, encodedInput, encodedOptions,
		runtime.ApplicationVersion(), now, now)
	if err != nil {
		return "", fmt.Errorf("goravel-dbos: record transactional SQLite dispatch for workflow %q: %w", workflowName, err)
	}
	var existing struct {
		WorkflowID   string `gorm:"column:workflow_id"`
		WorkflowName string `gorm:"column:workflow_name"`
		QueueName    string `gorm:"column:queue_name"`
	}
	if err := tx.ScanRaw(`SELECT workflow_id, workflow_name, queue_name FROM goravel_dbos_outbox WHERE workflow_id = ?`, &existing, options.workflowID); err != nil {
		return "", fmt.Errorf("goravel-dbos: verify transactional SQLite dispatch for workflow %q: %w", workflowName, err)
	}
	if existing.WorkflowID == "" {
		return "", fmt.Errorf("goravel-dbos: transactional SQLite dispatch for %q was not persisted", workflowName)
	}
	if existing.WorkflowName != workflowName || existing.QueueName != queueName {
		return "", fmt.Errorf("goravel-dbos: workflow ID %q is already assigned to %s/%s", options.workflowID, existing.QueueName, existing.WorkflowName)
	}
	return existing.WorkflowID, nil
}

func normalizeDatabaseDriver(driver string) string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "postgresql":
		return "postgres"
	case "sqlite3":
		return "sqlite"
	default:
		return strings.ToLower(strings.TrimSpace(driver))
	}
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func durationMilliseconds(value time.Duration) any {
	if value == 0 {
		return nil
	}
	return value.Milliseconds()
}

func timeMilliseconds(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UnixMilli()
}

func delayUntilMilliseconds(value time.Duration) any {
	if value == 0 {
		return nil
	}
	return time.Now().Add(value).UnixMilli()
}

func optionalPriority(value uint) any {
	if value == 0 {
		return nil
	}
	return int32(value)
}

func optionalJSON(values []string) (any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

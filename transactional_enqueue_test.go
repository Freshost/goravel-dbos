package goraveldbos

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/stretchr/testify/require"
)

type fakeRuntime struct {
	version string
	schema  string
	role    string
}

func (f fakeRuntime) Client() (dbossdk.Client, error) { return nil, errors.New("unused") }
func (f fakeRuntime) Role() string {
	if f.role == "" {
		return RoleProducer
	}
	return f.role
}
func (f fakeRuntime) ApplicationVersion() string { return f.version }
func (f fakeRuntime) DatabaseConnection() string { return "postgres" }
func (f fakeRuntime) DatabaseDriver() string     { return "" }
func (f fakeRuntime) DatabaseSchema() string     { return f.schema }

type fakeTransactionScanner struct {
	active       bool
	driver       string
	query        string
	values       []any
	err          error
	id           string
	workflowName string
	queueName    string
}

func (f *fakeTransactionScanner) InTransaction() bool { return f.active }
func (f *fakeTransactionScanner) Driver() string      { return f.driver }
func (f *fakeTransactionScanner) ScanRaw(query string, destination any, values ...any) error {
	f.query = query
	f.values = values
	if f.err != nil {
		return f.err
	}
	value := reflect.ValueOf(destination).Elem()
	value.FieldByName("WorkflowID").SetString(f.id)
	if field := value.FieldByName("WorkflowName"); field.IsValid() {
		field.SetString(f.workflowName)
		value.FieldByName("QueueName").SetString(f.queueName)
	}
	return nil
}
func (f *fakeTransactionScanner) ExecRaw(query string, values ...any) (int64, error) {
	f.query = query
	f.values = values
	if strings.Contains(query, "INSERT OR IGNORE INTO goravel_dbos_outbox") {
		f.id = values[0].(string)
		f.workflowName = values[1].(string)
		f.queueName = values[2].(string)
	}
	if f.err != nil {
		return 0, f.err
	}
	return 1, nil
}

func TestEnqueueWithTransactionScannerBuildsDBOSV1Call(t *testing.T) {
	runtime := fakeRuntime{version: "2026.08.1", schema: `tenant"dbos`}
	tx := &fakeTransactionScanner{active: true, driver: "postgres", id: "invoice:123"}
	definition := Define[map[string]string, struct{}]("billing.invoice_paid.v1", "billing")

	id, err := enqueueWithTransactionScanner(runtime, tx, definition, map[string]string{"invoiceId": "123"},
		WithTransactionWorkflowID("invoice:123"),
		WithTransactionTimeout(30*time.Second),
		WithTransactionDeduplicationID("invoice:123"),
		WithTransactionPriority(5),
		WithTransactionAuthenticatedRoles("admin", "billing"),
	)
	require.NoError(t, err)
	require.Equal(t, "invoice:123", id)
	require.Contains(t, tx.query, `"tenant""dbos".enqueue_workflow`)
	require.Len(t, tx.values, 15)
	require.Equal(t, "billing.invoice_paid.v1", tx.values[0])
	require.Equal(t, "billing", tx.values[1])
	require.JSONEq(t, `{"invoiceId":"123"}`, tx.values[2].(string))
	require.Equal(t, "invoice:123", tx.values[5])
	require.Equal(t, "2026.08.1", tx.values[6])
	require.Equal(t, int64(30000), tx.values[7])
	require.Equal(t, int32(5), tx.values[10])
	require.JSONEq(t, `["admin","billing"]`, tx.values[13].(string))
}

func TestTransactionEnqueueGeneratesWorkflowID(t *testing.T) {
	runtime := fakeRuntime{version: "dev", schema: "dbos"}
	tx := &fakeTransactionScanner{active: true, driver: "postgres"}
	tx.id = "generated-by-package"
	definition := Define[string, struct{}]("example.v1", "example")
	id, err := enqueueWithTransactionScanner(runtime, tx, definition, "input")
	require.NoError(t, err)
	require.Equal(t, "generated-by-package", id)
	require.NotEmpty(t, tx.values[5])
}

func TestTransactionEnqueueFailsClosed(t *testing.T) {
	definition := Define[string, struct{}]("example.v1", "example")
	_, err := enqueueWithTransactionScanner(
		fakeRuntime{role: RoleDisabled},
		&fakeTransactionScanner{active: true, driver: "postgres"},
		definition,
		"input",
	)
	require.ErrorIs(t, err, ErrDisabled)

	_, err = enqueueWithTransactionScanner(
		fakeRuntime{},
		&fakeTransactionScanner{active: false, driver: "postgres"},
		definition,
		"input",
	)
	require.ErrorContains(t, err, "active Goravel transaction")

	id, err := enqueueWithTransactionScanner(
		fakeRuntime{},
		&fakeTransactionScanner{active: true, driver: "sqlite"},
		definition,
		"input",
	)
	require.NoError(t, err)
	require.NotEmpty(t, id)
}

func TestTransactionEnqueueRejectsIncompatibleQueueOptions(t *testing.T) {
	definition := Define[string, struct{}]("example.v1", "example")
	_, err := enqueueWithTransactionScanner(
		fakeRuntime{},
		&fakeTransactionScanner{active: true, driver: "postgres"},
		definition,
		"input",
		WithTransactionDeduplicationID("same"),
		WithTransactionQueuePartitionKey("partition"),
	)
	require.ErrorContains(t, err, "cannot be combined")
}

func TestSQLiteTransactionEnqueueRejectsUnsupportedDeadline(t *testing.T) {
	definition := Define[string, struct{}]("example.v1", "example")
	_, err := enqueueWithTransactionScanner(
		fakeRuntime{},
		&fakeTransactionScanner{active: true, driver: "sqlite"},
		definition,
		"input",
		WithTransactionDeadline(time.Now().Add(time.Minute)),
	)
	require.ErrorContains(t, err, "use a timeout")
}

func TestSQLiteRetryDelayBacksOffWithJitterAndCaps(t *testing.T) {
	first := sqliteRetryDelay(1, time.Second, 10*time.Second)
	require.GreaterOrEqual(t, first, time.Second)
	require.LessOrEqual(t, first, 1250*time.Millisecond)
	require.Equal(t, 10*time.Second, sqliteRetryDelay(100, time.Second, 10*time.Second))
}

func TestTransactionEnqueueWrapsDatabaseErrorWithoutInputLeak(t *testing.T) {
	definition := Define[string, struct{}]("example.v1", "example")
	_, err := enqueueWithTransactionScanner(
		fakeRuntime{version: "dev", schema: "dbos"},
		&fakeTransactionScanner{active: true, driver: "postgres", err: errors.New("database unavailable")},
		definition,
		"secret input",
	)
	require.ErrorContains(t, err, "database unavailable")
	require.False(t, strings.Contains(err.Error(), "secret input"))
}

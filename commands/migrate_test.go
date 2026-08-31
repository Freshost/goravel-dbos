package commands

import (
	"testing"

	mocksconsole "github.com/goravel/framework/mocks/console"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	"github.com/stretchr/testify/require"
)

type migrationRuntime struct {
	prepared bool
	closed   bool
	role     string
}

func (r *migrationRuntime) Prepare() error           { r.prepared = true; return nil }
func (r *migrationRuntime) Shutdown() error          { r.closed = true; return nil }
func (r *migrationRuntime) Role() string             { return r.role }
func (*migrationRuntime) DatabaseConnection() string { return "primary" }
func (*migrationRuntime) DatabaseDriver() string     { return "postgres" }
func (*migrationRuntime) DatabaseSchema() string     { return "dbos" }

func TestMigratePreparesAndClosesRuntime(t *testing.T) {
	app := mocksfoundation.NewApplication(t)
	ctx := mocksconsole.NewContext(t)
	runtime := &migrationRuntime{role: "producer"}
	app.EXPECT().Make("freshost.dbos").Return(runtime, nil).Once()
	ctx.EXPECT().TwoColumnDetail("Goravel database connection", "primary").Once()
	ctx.EXPECT().TwoColumnDetail("Database driver", "postgres").Once()
	ctx.EXPECT().TwoColumnDetail("Database schema", "dbos").Once()
	ctx.EXPECT().Success("DBOS system schema is ready").Once()

	command := NewMigrate(app)
	require.Equal(t, "dbos:migrate", command.Signature())
	require.NotEmpty(t, command.Description())
	require.NoError(t, command.Handle(ctx))
	require.True(t, runtime.prepared)
	require.True(t, runtime.closed)
}

func TestMigrateRejectsDisabledRuntime(t *testing.T) {
	app := mocksfoundation.NewApplication(t)
	ctx := mocksconsole.NewContext(t)
	runtime := &migrationRuntime{role: "disabled"}
	app.EXPECT().Make("freshost.dbos").Return(runtime, nil).Once()

	err := NewMigrate(app).Handle(ctx)
	require.ErrorContains(t, err, "disabled")
	require.False(t, runtime.prepared)
	require.False(t, runtime.closed)
}

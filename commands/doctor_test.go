package commands

import (
	"testing"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	mocksconsole "github.com/goravel/framework/mocks/console"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	"github.com/stretchr/testify/require"
)

type disabledRuntime struct{}

func (disabledRuntime) Client() (dbossdk.Client, error) { return nil, nil }
func (disabledRuntime) Role() string                    { return "disabled" }
func (disabledRuntime) ApplicationVersion() string      { return "" }
func (disabledRuntime) DatabaseConnection() string      { return "" }
func (disabledRuntime) DatabaseDriver() string          { return "" }
func (disabledRuntime) DatabaseSchema() string          { return "" }

func TestDoctorDisabledRuntime(t *testing.T) {
	app := mocksfoundation.NewApplication(t)
	ctx := mocksconsole.NewContext(t)
	runtime := disabledRuntime{}
	app.EXPECT().Make("freshost.dbos").Return(runtime, nil).Once()
	ctx.EXPECT().TwoColumnDetail("Role", "disabled").Once()
	ctx.EXPECT().TwoColumnDetail("Application version", "").Once()
	ctx.EXPECT().TwoColumnDetail("Goravel database connection", "").Once()
	ctx.EXPECT().TwoColumnDetail("Database driver", "").Once()
	ctx.EXPECT().TwoColumnDetail("Atomic transactional enqueue", "false").Once()
	ctx.EXPECT().Warning("DBOS is disabled; connectivity was not checked").Once()

	doctor := NewDoctor(app)
	require.Equal(t, "dbos:doctor", doctor.Signature())
	require.NotEmpty(t, doctor.Description())
	require.NoError(t, doctor.Handle(ctx))
}

func TestDoctorRejectsUnexpectedBinding(t *testing.T) {
	app := mocksfoundation.NewApplication(t)
	ctx := mocksconsole.NewContext(t)
	app.EXPECT().Make("freshost.dbos").Return("not a runtime", nil).Once()

	err := NewDoctor(app).Handle(ctx)
	require.ErrorContains(t, err, "unexpected type")
}

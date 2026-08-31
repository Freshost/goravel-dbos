package commands

import (
	"fmt"

	"github.com/goravel/framework/contracts/console"
	"github.com/goravel/framework/contracts/console/command"
	contractsfoundation "github.com/goravel/framework/contracts/foundation"
)

// Migrate initializes and upgrades only the DBOS SDK-owned system schema. It
// never runs or publishes migrations owned by the consuming application.
type Migrate struct {
	app contractsfoundation.Application
}

func NewMigrate(app contractsfoundation.Application) *Migrate { return &Migrate{app: app} }

func (m *Migrate) Signature() string { return "dbos:migrate" }
func (m *Migrate) Description() string {
	return "Initialize or upgrade the DBOS system schema"
}
func (m *Migrate) Extend() command.Extend { return command.Extend{} }

func (m *Migrate) Handle(ctx console.Context) error {
	value, err := m.app.Make("freshost.dbos")
	if err != nil {
		return fmt.Errorf("resolve goravel-dbos runtime: %w", err)
	}
	runtime, ok := value.(interface {
		Prepare() error
		Shutdown() error
		Role() string
		DatabaseConnection() string
		DatabaseDriver() string
		DatabaseSchema() string
	})
	if !ok {
		return fmt.Errorf("goravel-dbos binding has unexpected type %T", value)
	}
	if runtime.Role() == "disabled" {
		return fmt.Errorf("goravel-dbos is disabled; enable it with a non-disabled role before running dbos:migrate")
	}

	ctx.TwoColumnDetail("Goravel database connection", runtime.DatabaseConnection())
	ctx.TwoColumnDetail("Database driver", runtime.DatabaseDriver())
	if runtime.DatabaseDriver() == "postgres" {
		ctx.TwoColumnDetail("Database schema", runtime.DatabaseSchema())
	}
	if err := runtime.Prepare(); err != nil {
		return fmt.Errorf("migrate DBOS system schema: %w", err)
	}
	if err := runtime.Shutdown(); err != nil {
		return fmt.Errorf("close DBOS migration client: %w", err)
	}
	ctx.Success("DBOS system schema is ready")
	return nil
}

var _ console.Command = (*Migrate)(nil)

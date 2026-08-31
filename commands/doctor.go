package commands

import (
	"fmt"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/goravel/framework/contracts/console"
	"github.com/goravel/framework/contracts/console/command"
	contractsfoundation "github.com/goravel/framework/contracts/foundation"
)

// Command is kept as an aliasable local interface so the root package can
// register commands without importing Goravel console implementation details.
type Command = console.Command

type Doctor struct {
	app contractsfoundation.Application
}

func NewDoctor(app contractsfoundation.Application) *Doctor { return &Doctor{app: app} }

func (d *Doctor) Signature() string      { return "dbos:doctor" }
func (d *Doctor) Description() string    { return "Validate goravel-dbos configuration and connectivity" }
func (d *Doctor) Extend() command.Extend { return command.Extend{} }

func (d *Doctor) Handle(ctx console.Context) error {
	value, err := d.app.Make("freshost.dbos")
	if err != nil {
		return fmt.Errorf("resolve goravel-dbos runtime: %w", err)
	}
	runtime, ok := value.(interface {
		Client() (dbossdk.Client, error)
		Role() string
		ApplicationVersion() string
		DatabaseConnection() string
		DatabaseDriver() string
		DatabaseSchema() string
	})
	if !ok {
		return fmt.Errorf("goravel-dbos binding has unexpected type %T", value)
	}
	ctx.TwoColumnDetail("Role", runtime.Role())
	ctx.TwoColumnDetail("Application version", runtime.ApplicationVersion())
	ctx.TwoColumnDetail("Goravel database connection", runtime.DatabaseConnection())
	ctx.TwoColumnDetail("Database driver", runtime.DatabaseDriver())
	if runtime.DatabaseDriver() == "postgres" {
		ctx.TwoColumnDetail("Database support tier", "production")
		ctx.TwoColumnDetail("Database schema", runtime.DatabaseSchema())
	}
	ctx.TwoColumnDetail("Atomic transactional enqueue", fmt.Sprintf("%t", runtime.Role() != "disabled"))
	if runtime.DatabaseDriver() == "sqlite" {
		ctx.TwoColumnDetail("Database support tier", "experimental")
		ctx.Warning("SQLite support is experimental and not recommended for production")
		if diagnostics, ok := value.(interface {
			SQLiteDispatchStats() (int64, int64, int64, error)
		}); ok {
			pending, failed, oldest, statsErr := diagnostics.SQLiteDispatchStats()
			if statsErr != nil {
				return fmt.Errorf("inspect internal SQLite dispatch journal: %w", statsErr)
			}
			ctx.TwoColumnDetail("Pending SQLite dispatches", fmt.Sprintf("%d", pending))
			ctx.TwoColumnDetail("Failed SQLite dispatches", fmt.Sprintf("%d", failed))
			if oldest > 0 {
				ctx.TwoColumnDetail("Oldest pending SQLite dispatch", time.UnixMilli(oldest).Format(time.RFC3339))
			}
		}
	}
	if runtime.Role() == "disabled" {
		ctx.Warning("DBOS is disabled; connectivity was not checked")
		return nil
	}
	client, err := runtime.Client()
	if err != nil {
		return fmt.Errorf("connect to DBOS system database: %w", err)
	}
	queues, err := dbossdk.ListQueues(client)
	if err != nil {
		return fmt.Errorf("list DBOS queues: %w", err)
	}
	ctx.TwoColumnDetail("Registered queues", fmt.Sprintf("%d", len(queues)))
	ctx.Success("goravel-dbos configuration and connectivity are healthy")
	return nil
}

var _ console.Command = (*Doctor)(nil)

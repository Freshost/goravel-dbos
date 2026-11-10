// This CI-only fixture leaves a real DBOS 1.0 database with pending workflows.
// Build it in CI, then point GORAVEL_DBOS_V1_FIXTURE at the resulting binary.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
)

func workflow(ctx dbos.Context, input string) (string, error) {
	prepared, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		return input + ":prepared", nil
	}, dbos.WithStepName("prepare"))
	if err != nil {
		return "", err
	}
	if err := dbos.SetEvent(ctx, "ready", true); err != nil {
		return "", err
	}
	approval, err := dbos.Recv[string](ctx, "approval", time.Hour)
	return prepared + ":" + approval, err
}

func seed() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	databaseURL := os.Getenv("GORAVEL_DBOS_TEST_DATABASE_URL")
	schema := os.Args[1]
	executor, err := dbos.NewContext(ctx, dbos.Config{
		AppName: "goravel-dbos-upgrade", ApplicationVersion: "upgrade-v1",
		DatabaseURL: databaseURL, DatabaseSchema: schema,
	})
	if err != nil {
		return err
	}
	// Deliberately omit Shutdown: exiting simulates a process crash. DBOS 1.0
	// durably cancelled workflows during graceful shutdown (fixed in 1.1).
	if _, err := dbos.RegisterQueue(executor, "upgrade"); err != nil {
		return err
	}
	dbos.RegisterWorkflow(executor, workflow, dbos.WithWorkflowName("upgrade.workflow.v1"))
	if err := dbos.Launch(executor); err != nil {
		return err
	}
	if _, err := dbos.Enqueue[string](executor, "upgrade", "upgrade.workflow.v1", "sdk",
		dbos.WithEnqueueWorkflowID("upgrade-sdk")); err != nil {
		return err
	}
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer connection.Close(ctx)
	// The old Goravel adapter used this public SQL function and portable JSON.
	_, err = connection.Exec(ctx, "SELECT "+pgx.Identifier{schema}.Sanitize()+`.enqueue_workflow(
		'upgrade.workflow.v1', 'upgrade', ARRAY['"sql"'::json],
		workflow_id => 'upgrade-sql', app_version => 'upgrade-v1')`)
	if err != nil {
		return err
	}
	for _, id := range []string{"upgrade-sdk", "upgrade-sql"} {
		if _, err := dbos.GetEvent[bool](executor, id, "ready", 20*time.Second); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := seed(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

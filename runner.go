package goraveldbos

import (
	"fmt"
	"strings"

	contractsfoundation "github.com/goravel/framework/contracts/foundation"
)

const RunnerSignature = "freshost:dbos"

type runner struct {
	app contractsfoundation.Application
}

func (r *runner) Signature() string { return RunnerSignature }

func (r *runner) ShouldRun() bool {
	config := r.app.MakeConfig()
	return config != nil && config.GetBool("dbos.enabled", true) && strings.ToLower(strings.TrimSpace(config.GetString("dbos.role", RoleProducer))) != RoleDisabled
}

func (r *runner) Run() error {
	runtime, err := Resolve(r.app)
	if err != nil {
		return err
	}
	if err := runtime.Prepare(); err != nil {
		return fmt.Errorf("prepare DBOS system database: %w", err)
	}
	if runtime.Role() == RoleWorker || runtime.Role() == RoleEmbedded {
		if err := runtime.Start(); err != nil {
			return fmt.Errorf("start DBOS runtime: %w", err)
		}
	}
	if err := runtime.startSQLiteRelay(); err != nil {
		return fmt.Errorf("start SQLite DBOS relay: %w", err)
	}
	<-r.app.Context().Done()
	return nil
}

func (r *runner) Shutdown() error {
	runtime, err := Resolve(r.app)
	if err != nil {
		return err
	}
	return runtime.Shutdown()
}

// Stop DBOS before default-priority application dependencies such as plugin
// processes. Applications may choose earlier priorities for ingress runners.
func (r *runner) ShutdownPriority() int { return -100 }

var _ contractsfoundation.RunnerWithShutdownPriority = (*runner)(nil)

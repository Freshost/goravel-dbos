// Package goraveldbos integrates DBOS Transact with Goravel's service
// container and runner lifecycle while leaving workflow ownership in the host
// application.
package goraveldbos

import (
	"slices"
	"strings"

	"github.com/goravel/framework/contracts/binding"
	contractsfoundation "github.com/goravel/framework/contracts/foundation"

	"github.com/freshost/goravel-dbos/commands"
)

const (
	PackageName = "github.com/freshost/goravel-dbos"
	Name        = "Goravel DBOS"
)

// App supports the conventional package facade. Prefer Resolve(app) in code
// that can receive a concrete application instance.
var App contractsfoundation.Application

type ServiceProvider struct{}

func (p *ServiceProvider) Relationship() binding.Relationship {
	return binding.Relationship{
		Bindings:     []string{Binding},
		Dependencies: []string{binding.Config, binding.Log, binding.Orm},
	}
}

func (p *ServiceProvider) Register(app contractsfoundation.Application) {
	App = app
	app.Singleton(Binding, func(app contractsfoundation.Application) (any, error) {
		return NewRuntime(app)
	})
}

func (p *ServiceProvider) Boot(app contractsfoundation.Application) {
	app.Commands([]commands.Command{commands.NewDoctor(app)})
	app.Publishes(PackageName, map[string]string{
		"setup/config/dbos.go": app.ConfigPath("dbos.go"),
	}, "goravel-dbos")

	config := app.MakeConfig()
	if config == nil || !config.GetBool("dbos.enabled", true) || strings.ToLower(strings.TrimSpace(config.GetString("dbos.role", RoleProducer))) != RoleWorker || !config.GetBool("dbos.worker.disable_goravel_runners", true) {
		return
	}
	disabled := config.GetStringSlice("app.disabled_runners", []string{})
	for _, signature := range config.GetStringSlice("dbos.worker.disabled_runners", defaultWorkerDisabledRunners) {
		if signature != "" && !slices.Contains(disabled, signature) {
			disabled = append(disabled, signature)
		}
	}
	config.Add("app.disabled_runners", disabled)
}

func (p *ServiceProvider) Runners(app contractsfoundation.Application) []contractsfoundation.Runner {
	return []contractsfoundation.Runner{&runner{app: app}}
}

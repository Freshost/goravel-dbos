package goraveldbos

import (
	"fmt"

	contractsfoundation "github.com/goravel/framework/contracts/foundation"
)

// Binding is the Goravel service-container key for the package runtime.
const Binding = "freshost.dbos"

// Resolve returns the package runtime from a specific Goravel application.
func Resolve(app contractsfoundation.Application) (*Runtime, error) {
	if app == nil {
		return nil, fmt.Errorf("goravel-dbos: application is required")
	}
	value, err := app.Make(Binding)
	if err != nil {
		return nil, fmt.Errorf("goravel-dbos: resolve runtime: %w", err)
	}
	runtime, ok := value.(*Runtime)
	if !ok {
		return nil, fmt.Errorf("goravel-dbos: binding %q has unexpected type %T", Binding, value)
	}
	return runtime, nil
}

// Register adds an application registrar during provider boot.
func Register(app contractsfoundation.Application, registrar Registrar) error {
	runtime, err := Resolve(app)
	if err != nil {
		return err
	}
	return runtime.AddRegistrar(registrar)
}

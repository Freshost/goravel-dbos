package facades

import (
	"fmt"

	"github.com/freshost/goravel-dbos"
)

// DBOS resolves the package runtime from the application most recently booted
// with goraveldbos.ServiceProvider.
func DBOS() (*goraveldbos.Runtime, error) {
	if goraveldbos.App == nil {
		return nil, fmt.Errorf("goravel-dbos: service provider is not registered")
	}
	return goraveldbos.Resolve(goraveldbos.App)
}

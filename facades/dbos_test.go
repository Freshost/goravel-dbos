package facades

import (
	"testing"

	goraveldbos "github.com/freshost/goravel-dbos"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	"github.com/stretchr/testify/require"
)

func TestDBOSRequiresRegisteredProvider(t *testing.T) {
	previous := goraveldbos.App
	t.Cleanup(func() { goraveldbos.App = previous })
	goraveldbos.App = nil

	_, err := DBOS()
	require.ErrorContains(t, err, "service provider is not registered")
}

func TestDBOSResolvesRuntimeFromApplication(t *testing.T) {
	previous := goraveldbos.App
	t.Cleanup(func() { goraveldbos.App = previous })
	app := mocksfoundation.NewApplication(t)
	runtime := &goraveldbos.Runtime{}
	app.EXPECT().Make(goraveldbos.Binding).Return(runtime, nil).Once()
	goraveldbos.App = app

	resolved, err := DBOS()
	require.NoError(t, err)
	require.Same(t, runtime, resolved)
}

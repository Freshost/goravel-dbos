package testing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeReportsMissingClientAndTracksCalls(t *testing.T) {
	runtime := &Runtime{
		RoleValue:       "producer",
		VersionValue:    "v1",
		ConnectionValue: "primary",
		DriverValue:     "postgres",
		SchemaValue:     "dbos",
	}

	_, err := runtime.Client()
	require.ErrorContains(t, err, "no ClientValue configured")
	require.Equal(t, 1, runtime.ClientCalls)
	require.Equal(t, "producer", runtime.Role())
	require.Equal(t, "v1", runtime.ApplicationVersion())
	require.Equal(t, "primary", runtime.DatabaseConnection())
	require.Equal(t, "postgres", runtime.DatabaseDriver())
	require.Equal(t, "dbos", runtime.DatabaseSchema())

	runtime.ClientError = context.Canceled
	_, err = runtime.Client()
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, runtime.ClientCalls)
}

package goraveldbos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefineCreatesTypedStableContract(t *testing.T) {
	definition := Define[string, int]("example.workflow.v1", "example")
	require.Equal(t, "example.workflow.v1", definition.Name())
	require.Equal(t, "example", definition.Queue())
}

func TestDefineRejectsEmptyNameOrQueue(t *testing.T) {
	require.Panics(t, func() { Define[string, int]("", "queue") })
	require.Panics(t, func() { Define[string, int]("workflow", "") })
}

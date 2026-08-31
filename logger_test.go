package goraveldbos

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogAttributesKeepTheirOriginalGroup(t *testing.T) {
	base := &goravelLogHandler{}
	withRequest := base.WithAttrs([]slog.Attr{slog.String("request_id", "123")}).(*goravelLogHandler)
	withDBOSGroup := withRequest.WithGroup("workflow").(*goravelLogHandler)
	withWorkflow := withDBOSGroup.WithAttrs([]slog.Attr{slog.String("id", "invoice:1")}).(*goravelLogHandler)

	data := map[string]any{}
	for _, attr := range withWorkflow.attrs {
		withWorkflow.addAttr(data, attr.group, attr.attr)
	}

	require.Equal(t, "123", data["request_id"])
	require.Equal(t, "invoice:1", data["workflow.id"])
}

func TestLogAttributesFlattenNestedGroups(t *testing.T) {
	handler := &goravelLogHandler{group: "dbos"}
	data := map[string]any{}
	handler.addAttr(data, handler.group, slog.Group("workflow", slog.String("id", "invoice:1")))
	require.Equal(t, "invoice:1", data["dbos.workflow.id"])
}

func TestLogAttributesRedactConnectionAndCredentials(t *testing.T) {
	handler := &goravelLogHandler{}
	data := map[string]any{}
	handler.addAttr(data, "", slog.String("database_url", "postgresql://user:secret@example/db"))
	handler.addAttr(data, "conductor", slog.String("api_key", "private"))
	require.Equal(t, "[REDACTED]", data["database_url"])
	require.Equal(t, "[REDACTED]", data["conductor.api_key"])
}

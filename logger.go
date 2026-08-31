package goraveldbos

import (
	"context"
	"log/slog"
	"strings"

	contractslog "github.com/goravel/framework/contracts/log"
)

type goravelLogHandler struct {
	logger contractslog.Log
	level  slog.Level
	attrs  []groupedAttr
	group  string
}

type groupedAttr struct {
	group string
	attr  slog.Attr
}

func newDBOSLogger(logger contractslog.Log, level string) *slog.Logger {
	minimum := slog.LevelInfo
	switch level {
	case "debug":
		minimum = slog.LevelDebug
	case "warn":
		minimum = slog.LevelWarn
	case "error":
		minimum = slog.LevelError
	}
	return slog.New(&goravelLogHandler{logger: logger, level: minimum})
}

func (h *goravelLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.logger != nil && level >= h.level
}

func (h *goravelLogHandler) Handle(ctx context.Context, record slog.Record) error {
	data := make(map[string]any, len(h.attrs)+record.NumAttrs())
	for _, attr := range h.attrs {
		h.addAttr(data, attr.group, attr.attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		h.addAttr(data, h.group, attr)
		return true
	})
	writer := h.logger.WithContext(ctx).With(data).In("dbos")
	switch {
	case record.Level >= slog.LevelError:
		writer.Error(record.Message)
	case record.Level >= slog.LevelWarn:
		writer.Warning(record.Message)
	case record.Level >= slog.LevelInfo:
		writer.Info(record.Message)
	default:
		writer.Debug(record.Message)
	}
	return nil
}

func (h *goravelLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append([]groupedAttr{}, h.attrs...)
	for _, attr := range attrs {
		clone.attrs = append(clone.attrs, groupedAttr{group: h.group, attr: attr})
	}
	return &clone
}

func (h *goravelLogHandler) WithGroup(name string) slog.Handler {
	clone := *h
	if clone.group == "" {
		clone.group = name
	} else {
		clone.group += "." + name
	}
	return &clone
}

func (h *goravelLogHandler) addAttr(data map[string]any, group string, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	if attr.Value.Kind() == slog.KindGroup {
		nestedGroup := joinGroup(group, attr.Key)
		for _, child := range attr.Value.Group() {
			h.addAttr(data, nestedGroup, child)
		}
		return
	}
	key := attr.Key
	if group != "" {
		key = group + "." + key
	}
	if sensitiveLogAttribute(key) {
		data[key] = "[REDACTED]"
		return
	}
	data[key] = attr.Value.Any()
}

func sensitiveLogAttribute(key string) bool {
	key = strings.ToLower(key)
	for _, fragment := range []string{"database_url", "api_key", "password", "secret", "token"} {
		if strings.Contains(key, fragment) {
			return true
		}
	}
	return false
}

func joinGroup(parent, child string) string {
	if parent == "" {
		return child
	}
	if child == "" {
		return parent
	}
	return parent + "." + child
}

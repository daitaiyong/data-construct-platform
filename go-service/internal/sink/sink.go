package sink

import "context"

type Record map[string]any

type Writer interface {
	WriteBatch(ctx context.Context, table string, records []Record) error
}

// MySQLWriter is intentionally an adapter boundary; add a concrete
// implementation when connection and schema ownership are defined.
type MySQLWriter struct{}

func (MySQLWriter) WriteBatch(context.Context, string, []Record) error {
	return ErrNotConfigured
}

var ErrNotConfigured = errNotConfigured{}

type errNotConfigured struct{}

func (errNotConfigured) Error() string { return "mysql sink is not configured" }

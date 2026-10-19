package dag

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusEnabled  Status = "enabled"
	StatusDisabled Status = "disabled"
)

type Definition struct {
	ID        string          `json:"dag_id"`
	Name      string          `json:"name"`
	DAG       json.RawMessage `json:"dag_content"`
	CronExpr  string          `json:"cron_expr,omitempty"`
	Status    Status          `json:"status"`
	Creator   string          `json:"creator"`
	CreatedAt time.Time       `json:"create_time"`
	UpdatedAt time.Time       `json:"update_time"`
}

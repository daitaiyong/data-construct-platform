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
	ID        string          `json:"dagId"`
	Name      string          `json:"name"`
	DAG       json.RawMessage `json:"dag"`
	CronExpr  string          `json:"cronExpr,omitempty"`
	Status    Status          `json:"status"`
	Creator   string          `json:"creator"`
	CreatedAt time.Time       `json:"createTime"`
	UpdatedAt time.Time       `json:"updateTime"`
}

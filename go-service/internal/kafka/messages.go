package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

type TaskMessage struct {
	MessageID    string    `json:"msg_id"`
	JobID        string    `json:"job_id"`
	TaskID       string    `json:"task_id"`
	NodeID       string    `json:"node_id"`
	TemplateJSON string    `json:"template_json"`
	ContextKey   string    `json:"context_key"`
	BatchCount   int64     `json:"batch_count"`
	SinkKey      string    `json:"sink_key"`
	FailStrategy string    `json:"fail_strategy"`
	RetryTimes   uint32    `json:"retry_times"`
	MaxRetry     uint32    `json:"max_retry"`
	CreateAt     time.Time `json:"create_at"`
}

func (message TaskMessage) Validate() error {
	for name, value := range map[string]string{
		"msg_id":        message.MessageID,
		"job_id":        message.JobID,
		"task_id":       message.TaskID,
		"node_id":       message.NodeID,
		"template_json": message.TemplateJSON,
		"context_key":   message.ContextKey,
		"sink_key":      message.SinkKey,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not be empty", name)
		}
	}
	if message.BatchCount <= 0 {
		return errors.New("batch_count must be positive")
	}
	if message.FailStrategy != "abort_dag" && message.FailStrategy != "continue" {
		return fmt.Errorf("invalid fail_strategy %q", message.FailStrategy)
	}
	if message.RetryTimes > message.MaxRetry {
		return errors.New("retry_times must not exceed max_retry")
	}
	if message.CreateAt.IsZero() {
		return errors.New("create_at must not be empty")
	}
	var templateObject map[string]json.RawMessage
	if err := json.Unmarshal([]byte(message.TemplateJSON), &templateObject); err != nil {
		return fmt.Errorf("template_json must be a JSON object: %w", err)
	}
	if templateObject == nil {
		return errors.New("template_json must be a JSON object")
	}
	return nil
}

type EventType string

const (
	EventTaskStart   EventType = "task_start"
	EventTaskSuccess EventType = "task_success"
	EventTaskFail    EventType = "task_fail"
	EventJobSuccess  EventType = "job_success"
	EventJobFail     EventType = "job_fail"
	EventJobCancel   EventType = "job_cancel"
)

type EventPayload struct {
	GeneratedRows int64  `json:"generated_rows"`
	WriteCostMS   int64  `json:"write_cost_ms"`
	ErrorMessage  string `json:"error_msg"`
	Stack         string `json:"stack"`
}

type EventMessage struct {
	EventID   string       `json:"event_id"`
	EventType EventType    `json:"event_type"`
	JobID     string       `json:"job_id"`
	TaskID    string       `json:"task_id,omitempty"`
	NodeID    string       `json:"node_id,omitempty"`
	Payload   EventPayload `json:"payload"`
	Timestamp time.Time    `json:"timestamp"`
}

func (message EventMessage) Validate() error {
	if strings.TrimSpace(message.EventID) == "" {
		return errors.New("event_id must not be empty")
	}
	if strings.TrimSpace(message.JobID) == "" {
		return errors.New("job_id must not be empty")
	}
	switch message.EventType {
	case EventTaskStart, EventTaskSuccess, EventTaskFail:
		if strings.TrimSpace(message.TaskID) == "" || strings.TrimSpace(message.NodeID) == "" {
			return errors.New("task events require task_id and node_id")
		}
	case EventJobSuccess, EventJobFail, EventJobCancel:
	default:
		return fmt.Errorf("invalid event_type %q", message.EventType)
	}
	if message.Timestamp.IsZero() {
		return errors.New("timestamp must not be empty")
	}
	return nil
}

func DecodeTaskMessage(data []byte) (TaskMessage, error) {
	var message TaskMessage
	if err := decodeSingleJSON(data, &message); err != nil {
		return TaskMessage{}, err
	}
	if err := message.Validate(); err != nil {
		return TaskMessage{}, err
	}
	return message, nil
}

func DecodeEventMessage(data []byte) (EventMessage, error) {
	var message EventMessage
	if err := decodeSingleJSON(data, &message); err != nil {
		return EventMessage{}, err
	}
	if err := message.Validate(); err != nil {
		return EventMessage{}, err
	}
	return message, nil
}

func PublishTask(ctx context.Context, producer Producer, message TaskMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}
	return publishJSON(ctx, producer, TaskTopic, message.JobID, message)
}

func PublishEvent(ctx context.Context, producer Producer, message EventMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}
	return publishJSON(ctx, producer, EventTopic, message.JobID, message)
}

func publishJSON(ctx context.Context, producer Producer, topic, jobID string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return producer.Publish(ctx, topic, Message{Key: []byte(jobID), Value: data})
}

func decodeSingleJSON(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("message must contain a single JSON value")
		}
		return err
	}
	return nil
}

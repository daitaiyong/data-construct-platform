package kafka

import (
	"context"
	"errors"
)

var ErrNotConfigured = errors.New("kafka adapter is not configured")

const (
	TaskTopic  = "data-construct-task"
	EventTopic = "data-construct-event"

	EventStateConsumerGroup = "data-construct-state"
	EventMetricsGroup       = "data-construct-metrics"
	EventLogsGroup          = "data-construct-logs"
)

type Message struct {
	Key   []byte
	Value []byte
}

type Producer interface {
	Publish(ctx context.Context, topic string, message Message) error
}

type Consumer interface {
	Consume(ctx context.Context, topic string, handler func(context.Context, Message) error) error
}

type Adapter struct{}

func (Adapter) Publish(context.Context, string, Message) error { return ErrNotConfigured }
func (Adapter) Consume(context.Context, string, func(context.Context, Message) error) error {
	return ErrNotConfigured
}

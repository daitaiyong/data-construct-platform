package job

import (
	"context"
	"errors"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/kafka"
)

var ErrEventStoreNotConfigured = errors.New("atomic event state store is not configured")

// AtomicEventStore must persist event de-duplication and all resulting Job/Task
// state changes in one database transaction.
type AtomicEventStore interface {
	ApplyEvent(ctx context.Context, event kafka.EventMessage) (EventActions, error)
}

type EventConsumer struct {
	store AtomicEventStore
}

func NewEventConsumer(store AtomicEventStore) (*EventConsumer, error) {
	if store == nil {
		return nil, ErrEventStoreNotConfigured
	}
	return &EventConsumer{store: store}, nil
}

func (consumer *EventConsumer) Handle(ctx context.Context, message kafka.Message) (EventActions, error) {
	event, err := kafka.DecodeEventMessage(message.Value)
	if err != nil {
		return EventActions{}, err
	}
	if len(message.Key) == 0 || string(message.Key) != event.JobID {
		return EventActions{}, errors.New("Kafka event key must equal job_id")
	}
	return consumer.store.ApplyEvent(ctx, event)
}

// MemoryEventStore is a process-local reference implementation for tests and
// development. Production adapters must implement AtomicEventStore with a DB
// transaction and durable event-id de-duplication.
type MemoryEventStore struct {
	executions map[string]*Execution
}

func NewMemoryEventStore(executions ...*Execution) *MemoryEventStore {
	store := &MemoryEventStore{executions: make(map[string]*Execution, len(executions))}
	for _, execution := range executions {
		if execution != nil {
			store.executions[execution.JobID] = execution
		}
	}
	return store
}

func (store *MemoryEventStore) ApplyEvent(_ context.Context, event kafka.EventMessage) (EventActions, error) {
	execution, exists := store.executions[event.JobID]
	if !exists {
		return EventActions{}, ErrNotFound
	}
	return execution.ApplyEvent(event)
}

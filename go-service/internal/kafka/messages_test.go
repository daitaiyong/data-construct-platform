package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type capturedMessage struct {
	topic   string
	message Message
}

type captureProducer struct {
	messages []capturedMessage
}

func (p *captureProducer) Publish(_ context.Context, topic string, message Message) error {
	p.messages = append(p.messages, capturedMessage{topic: topic, message: message})
	return nil
}

func TestPublishTaskUsesJobIDAsKafkaKey(t *testing.T) {
	producer := &captureProducer{}
	task := validTaskMessage()
	if err := PublishTask(context.Background(), producer, task); err != nil {
		t.Fatal(err)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("published %d messages, want 1", len(producer.messages))
	}
	published := producer.messages[0]
	if published.topic != TaskTopic || string(published.message.Key) != task.JobID {
		t.Fatalf("published topic/key = %q/%q", published.topic, published.message.Key)
	}
	var decoded TaskMessage
	if err := json.Unmarshal(published.message.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != task {
		t.Fatalf("published task = %#v, want %#v", decoded, task)
	}
}

func TestDecodeTaskRejectsInvalidRetryAndUnknownFields(t *testing.T) {
	task := validTaskMessage()
	task.RetryTimes = 3
	if _, err := DecodeTaskMessage(mustJSON(t, task)); err == nil {
		t.Fatal("expected retry_times validation error")
	}
	data := append(mustJSON(t, validTaskMessage())[:len(mustJSON(t, validTaskMessage()))-1], []byte(`,"extra":true}`)...)
	if _, err := DecodeTaskMessage(data); err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestPublishEventUsesJobIDAsKafkaKey(t *testing.T) {
	producer := &captureProducer{}
	event := EventMessage{
		EventID:   "event-1",
		EventType: EventTaskSuccess,
		JobID:     "job-1",
		TaskID:    "task-1",
		NodeID:    "node-1",
		Payload:   EventPayload{GeneratedRows: 20, WriteCostMS: 17},
		Timestamp: time.Now().UTC(),
	}
	if err := PublishEvent(context.Background(), producer, event); err != nil {
		t.Fatal(err)
	}
	if got := producer.messages[0]; got.topic != EventTopic || string(got.message.Key) != event.JobID {
		t.Fatalf("published topic/key = %q/%q", got.topic, got.message.Key)
	}
}

func validTaskMessage() TaskMessage {
	return TaskMessage{
		MessageID:    "msg-1",
		JobID:        "job-1",
		TaskID:       "task-1",
		NodeID:       "node-1",
		TemplateJSON: `{"target_table":"users"}`,
		ContextKey:   "dc:job:job-1:context",
		BatchCount:   10,
		SinkKey:      "mysql-test",
		FailStrategy: "abort_dag",
		MaxRetry:     2,
		CreateAt:     time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC),
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

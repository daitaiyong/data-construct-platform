package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/kafka"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/pb"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/redis"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/sink"
	"google.golang.org/grpc"
)

type fakeGenerator struct {
	pb.UnimplementedTemplateEngineServiceServer
	response *pb.GenerateResponse
}

func (g fakeGenerator) GenerateData(context.Context, *pb.GenerateRequest) (*pb.GenerateResponse, error) {
	return g.response, nil
}

type fakeContextCache struct {
	values map[string]string
}

func (c fakeContextCache) GetAll(context.Context, string) (map[string]string, error) {
	return c.values, nil
}
func (fakeContextCache) SetFields(context.Context, string, map[string]string, time.Duration) error {
	return nil
}

type fakeLease struct{}

func (fakeLease) Renew(context.Context, time.Duration) error { return nil }
func (fakeLease) Release(context.Context) error              { return nil }

type fakeLocker struct {
	key string
	ttl time.Duration
}

func (l *fakeLocker) Lock(_ context.Context, key string, ttl time.Duration) (redis.Lease, error) {
	l.key = key
	l.ttl = ttl
	return fakeLease{}, nil
}

type fakeWriter struct {
	taskID  string
	table   string
	records []sink.Record
}

func (w *fakeWriter) WriteBatch(_ context.Context, taskID, table string, records []sink.Record) error {
	w.taskID = taskID
	w.table = table
	w.records = records
	return nil
}

type fakeSinkResolver struct {
	key    string
	writer *fakeWriter
}

func (r fakeSinkResolver) Resolve(key string) (sink.Writer, error) {
	if key != r.key {
		return nil, errors.New("unexpected sink key")
	}
	return r.writer, nil
}

type fakeEventProducer struct {
	messages []kafka.Message
	topics   []string
}

func (p *fakeEventProducer) Publish(_ context.Context, topic string, message kafka.Message) error {
	p.topics = append(p.topics, topic)
	p.messages = append(p.messages, message)
	return nil
}

func TestWorkerConsumesTaskAndPublishesEvents(t *testing.T) {
	writer := &fakeWriter{}
	producer := &fakeEventProducer{}
	locker := &fakeLocker{}
	worker := newTestWorker(t, fakeGenerator{response: &pb.GenerateResponse{
		Success:  true,
		RowsJson: []string{`{"id":"1"}`, `{"id":"2"}`},
		Total:    2,
	}}, fakeContextCache{values: map[string]string{"node-001.user_id_list": `["1"]`}}, fakeSinkResolver{key: "mysql-test", writer: writer}, producer, locker)

	task := validTask()
	if err := worker.Consume(context.Background(), kafka.Message{Key: []byte(task.JobID), Value: mustTaskJSON(t, task)}); err != nil {
		t.Fatal(err)
	}
	if writer.taskID != task.TaskID || writer.table != "target_users" || len(writer.records) != 2 || writer.records[1]["id"] != "2" {
		t.Fatalf("sink write task/table/records = %q/%q/%v", writer.taskID, writer.table, writer.records)
	}
	if locker.key != redis.TaskLockKey(task.TaskID) || locker.ttl != redis.TaskLockTTL {
		t.Fatalf("task lock key/TTL = %q/%s", locker.key, locker.ttl)
	}
	if len(producer.messages) != 2 {
		t.Fatalf("published %d events, want task_start and task_success", len(producer.messages))
	}
	for index, wantType := range []kafka.EventType{kafka.EventTaskStart, kafka.EventTaskSuccess} {
		if producer.topics[index] != kafka.EventTopic || string(producer.messages[index].Key) != task.JobID {
			t.Fatalf("event %d topic/key = %q/%q", index, producer.topics[index], producer.messages[index].Key)
		}
		event, err := kafka.DecodeEventMessage(producer.messages[index].Value)
		if err != nil {
			t.Fatal(err)
		}
		if event.EventType != wantType || event.TaskID != task.TaskID {
			t.Fatalf("event %d = %#v, want type %q", index, event, wantType)
		}
		if wantType == kafka.EventTaskSuccess && event.Payload.GeneratedRows != 2 {
			t.Fatalf("generated_rows = %d, want 2", event.Payload.GeneratedRows)
		}
	}
}

func TestWorkerPublishesFailureEventOnGenerationError(t *testing.T) {
	producer := &fakeEventProducer{}
	worker := newTestWorker(t, fakeGenerator{response: &pb.GenerateResponse{
		Success:  false,
		ErrorMsg: "invalid field rule",
	}}, fakeContextCache{values: map[string]string{}}, fakeSinkResolver{key: "mysql-test", writer: &fakeWriter{}}, producer, nil)

	task := validTask()
	err := worker.Consume(context.Background(), kafka.Message{Key: []byte(task.JobID), Value: mustTaskJSON(t, task)})
	if err == nil {
		t.Fatal("Consume() error = nil, want generation failure")
	}
	if len(producer.messages) != 2 {
		t.Fatalf("published %d events, want task_start and task_fail", len(producer.messages))
	}
	event, decodeErr := kafka.DecodeEventMessage(producer.messages[1].Value)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if event.EventType != kafka.EventTaskFail || event.Payload.ErrorMessage == "" || event.Payload.Stack == "" {
		t.Fatalf("failure event payload = %#v", event)
	}
}

func TestWorkerRequiresKafkaKeyToMatchJobID(t *testing.T) {
	worker := newTestWorker(t, fakeGenerator{}, fakeContextCache{}, fakeSinkResolver{}, &fakeEventProducer{}, nil)
	task := validTask()
	err := worker.Consume(context.Background(), kafka.Message{Key: []byte("different-job"), Value: mustTaskJSON(t, task)})
	if err == nil {
		t.Fatal("Consume() error = nil, want key mismatch")
	}
}

func newTestWorker(t *testing.T, generator fakeGenerator, cache fakeContextCache, sinks fakeSinkResolver, producer *fakeEventProducer, locker *fakeLocker) *Worker {
	t.Helper()
	if locker == nil {
		locker = &fakeLocker{}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterTemplateEngineServiceServer(server, generator)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	worker, err := NewWorker(listener.Addr().String(), cache, locker, sinks, producer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = worker.Close()
	})
	return worker
}

func validTask() kafka.TaskMessage {
	return kafka.TaskMessage{
		MessageID:    "msg-1",
		JobID:        "job-1",
		TaskID:       "task-1",
		NodeID:       "node-1",
		TemplateJSON: `{"target_table":"target_users"}`,
		ContextKey:   "dc:job:job-1:context",
		BatchCount:   2,
		SinkKey:      "mysql-test",
		FailStrategy: "abort_dag",
		MaxRetry:     2,
		CreateAt:     time.Now().UTC(),
	}
}

func mustTaskJSON(t *testing.T, task kafka.TaskMessage) []byte {
	t.Helper()
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/id"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/kafka"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/pb"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/redis"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/sink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SinkResolver interface {
	Resolve(key string) (sink.Writer, error)
}

type Worker struct {
	connection *grpc.ClientConn
	generator  pb.TemplateEngineServiceClient
	contexts   redis.HashStore
	locks      redis.Locker
	sinks      SinkResolver
	events     kafka.Producer
}

type Consumer interface {
	Consume(ctx context.Context, topic string, handler func(context.Context, kafka.Message) error) error
}

func NewWorker(grpcAddr string, contexts redis.HashStore, locks redis.Locker, sinks SinkResolver, events kafka.Producer) (*Worker, error) {
	if contexts == nil || locks == nil || sinks == nil || events == nil {
		return nil, errors.New("worker requires Redis context and lock, sink, and Kafka event adapters")
	}
	connection, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to template engine: %w", err)
	}
	return &Worker{
		connection: connection,
		generator:  pb.NewTemplateEngineServiceClient(connection),
		contexts:   contexts,
		locks:      locks,
		sinks:      sinks,
		events:     events,
	}, nil
}

func (w *Worker) Close() error {
	return w.connection.Close()
}

func (w *Worker) Run(ctx context.Context, consumer Consumer) error {
	return consumer.Consume(ctx, kafka.TaskTopic, w.Consume)
}

// Consume handles one Kafka task message. Returning an error leaves retry or
// dead-letter policy to the Kafka consumer/scheduler adapter.
func (w *Worker) Consume(ctx context.Context, message kafka.Message) (err error) {
	task, err := kafka.DecodeTaskMessage(message.Value)
	if err != nil {
		return fmt.Errorf("decode task message: %w", err)
	}
	if len(message.Key) == 0 || string(message.Key) != task.JobID {
		return fmt.Errorf("Kafka message key must equal job_id %q", task.JobID)
	}

	lease, err := redis.AcquireTaskLock(ctx, w.locks, task.TaskID)
	if err != nil {
		return fmt.Errorf("acquire task lock %q: %w", task.TaskID, err)
	}
	if lease == nil {
		return errors.New("Redis task lock returned an empty lease")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if releaseErr := lease.Release(releaseCtx); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release task lock %q: %w", task.TaskID, releaseErr))
		}
	}()

	if err := w.publishEvent(ctx, task, kafka.EventTaskStart, kafka.EventPayload{}); err != nil {
		return fmt.Errorf("publish task_start event: %w", err)
	}
	payload, err := w.execute(ctx, task)
	if err != nil {
		payload := kafka.EventPayload{ErrorMessage: err.Error(), Stack: string(debug.Stack())}
		if publishErr := w.publishEvent(ctx, task, kafka.EventTaskFail, payload); publishErr != nil {
			return errors.Join(err, fmt.Errorf("publish task_fail event: %w", publishErr))
		}
		return err
	}
	if err := w.publishEvent(ctx, task, kafka.EventTaskSuccess, payload); err != nil {
		return fmt.Errorf("publish task_success event: %w", err)
	}
	return nil
}

func (w *Worker) execute(ctx context.Context, task kafka.TaskMessage) (kafka.EventPayload, error) {
	contextFields, err := w.contexts.GetAll(ctx, task.ContextKey)
	if err != nil {
		return kafka.EventPayload{}, fmt.Errorf("load DAG context %q: %w", task.ContextKey, err)
	}
	contextObject := make(map[string]any, len(contextFields))
	for name, encodedValue := range contextFields {
		var value any
		if err := json.Unmarshal([]byte(encodedValue), &value); err != nil {
			return kafka.EventPayload{}, fmt.Errorf("decode DAG context field %q: %w", name, err)
		}
		contextObject[name] = value
	}
	contextJSON, err := json.Marshal(contextObject)
	if err != nil {
		return kafka.EventPayload{}, fmt.Errorf("encode DAG context: %w", err)
	}

	response, err := w.generator.GenerateData(ctx, &pb.GenerateRequest{
		TemplateJson: task.TemplateJSON,
		ContextJson:  string(contextJSON),
		Count:        task.BatchCount,
	})
	if err != nil {
		return kafka.EventPayload{}, fmt.Errorf("generate task data: %w", err)
	}
	if !response.Success {
		return kafka.EventPayload{}, fmt.Errorf("template engine generation failed: %s", response.ErrorMsg)
	}
	if response.Total != task.BatchCount || int64(len(response.RowsJson)) != response.Total {
		return kafka.EventPayload{}, fmt.Errorf("template engine returned %d rows with total %d; expected %d", len(response.RowsJson), response.Total, task.BatchCount)
	}

	records := make([]sink.Record, len(response.RowsJson))
	for index, rowJSON := range response.RowsJson {
		var record sink.Record
		if err := json.Unmarshal([]byte(rowJSON), &record); err != nil || record == nil {
			return kafka.EventPayload{}, fmt.Errorf("generated row %d must be a JSON object", index)
		}
		records[index] = record
	}
	var templateData struct {
		TargetTable string `json:"target_table"`
	}
	if err := json.Unmarshal([]byte(task.TemplateJSON), &templateData); err != nil {
		return kafka.EventPayload{}, fmt.Errorf("decode template target: %w", err)
	}
	if templateData.TargetTable == "" {
		return kafka.EventPayload{}, errors.New("template target_table must not be empty")
	}
	writer, err := w.sinks.Resolve(task.SinkKey)
	if err != nil {
		return kafka.EventPayload{}, fmt.Errorf("resolve sink %q: %w", task.SinkKey, err)
	}
	writeStarted := time.Now()
	if err := writer.WriteBatch(ctx, task.TaskID, templateData.TargetTable, records); err != nil {
		return kafka.EventPayload{}, fmt.Errorf("write task batch to %q: %w", task.SinkKey, err)
	}
	return kafka.EventPayload{
		GeneratedRows: response.Total,
		WriteCostMS:   time.Since(writeStarted).Milliseconds(),
	}, nil
}

func (w *Worker) publishEvent(ctx context.Context, task kafka.TaskMessage, eventType kafka.EventType, payload kafka.EventPayload) error {
	eventID, err := id.New()
	if err != nil {
		return fmt.Errorf("generate event_id: %w", err)
	}
	return kafka.PublishEvent(ctx, w.events, kafka.EventMessage{
		EventID:   eventID,
		EventType: eventType,
		JobID:     task.JobID,
		TaskID:    task.TaskID,
		NodeID:    task.NodeID,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	})
}

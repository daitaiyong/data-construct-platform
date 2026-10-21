package job

import (
	"errors"
	"sync"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/id"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/redis"
)

var ErrNotFound = errors.New("job not found")

type Status string

const (
	StatusPending  Status = "pending"
	StatusRunning  Status = "running"
	StatusSuccess  Status = "success"
	StatusFailed   Status = "failed"
	StatusCanceled Status = "canceled"

	// Deprecated aliases retained for callers compiled against the initial scaffold.
	StatusQueued    = StatusPending
	StatusSucceeded = StatusSuccess
)

type Job struct {
	ID          string     `json:"job_id"`
	DAGID       string     `json:"dag_id"`
	Status      Status     `json:"status"`
	StartTime   *time.Time `json:"start_time,omitempty"`
	EndTime     *time.Time `json:"end_time,omitempty"`
	TriggerType string     `json:"trigger_type"`
	ContextKey  string     `json:"context_key"`
	CreatedAt   time.Time  `json:"create_time"`
}

type Repository interface {
	Create(dagID, triggerType string) (Job, error)
	Get(id string) (Job, error)
}

// MemoryRepository is a process-local placeholder for the future MySQL adapter.
type MemoryRepository struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{jobs: make(map[string]Job)}
}

func (r *MemoryRepository) Create(dagID, triggerType string) (Job, error) {
	jobID, err := id.New()
	if err != nil {
		return Job{}, err
	}
	now := time.Now().UTC()
	item := Job{
		ID:          jobID,
		DAGID:       dagID,
		Status:      StatusPending,
		TriggerType: triggerType,
		ContextKey:  redis.JobContextKey(jobID),
		CreatedAt:   now,
	}
	r.mu.Lock()
	r.jobs[item.ID] = item
	r.mu.Unlock()
	return item, nil
}

func (r *MemoryRepository) Get(id string) (Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return item, nil
}

type TaskStatus string

const (
	TaskStatusPending  TaskStatus = "pending"
	TaskStatusRunning  TaskStatus = "running"
	TaskStatusSuccess  TaskStatus = "success"
	TaskStatusFailed   TaskStatus = "failed"
	TaskStatusCanceled TaskStatus = "canceled"
	TaskStatusSkipped  TaskStatus = "skipped"

	// Deprecated alias retained for callers compiled against the initial scaffold.
	TaskStatusSucceeded = TaskStatusSuccess
)

type Task struct {
	ID           string     `json:"task_id"`
	JobID        string     `json:"job_id"`
	TemplateID   string     `json:"template_id"`
	NodeID       string     `json:"node_id"`
	Status       TaskStatus `json:"status"`
	ErrorMsg     string     `json:"error_msg,omitempty"`
	BatchCount   uint64     `json:"batch_count"`
	FailStrategy string     `json:"fail_strategy"`
	RetryTimes   uint32     `json:"retry_times"`
	MaxRetry     uint32     `json:"max_retry"`
	StartTime    *time.Time `json:"start_time,omitempty"`
	EndTime      *time.Time `json:"end_time,omitempty"`
}

type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

type JobLog struct {
	ID        string    `json:"log_id"`
	JobID     string    `json:"job_id"`
	TaskID    *string   `json:"task_id,omitempty"`
	Level     LogLevel  `json:"level"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"create_time"`
}

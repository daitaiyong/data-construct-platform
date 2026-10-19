package job

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/redis"
)

var ErrNotFound = errors.New("job not found")

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type Job struct {
	ID          string     `json:"jobId"`
	DAGID       string     `json:"dagId"`
	Status      Status     `json:"status"`
	StartTime   *time.Time `json:"startTime,omitempty"`
	EndTime     *time.Time `json:"endTime,omitempty"`
	TriggerType string     `json:"triggerType"`
	ContextKey  string     `json:"contextKey"`
	CreatedAt   time.Time  `json:"createTime"`
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
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return Job{}, err
	}
	now := time.Now().UTC()
	id := hex.EncodeToString(random)
	item := Job{
		ID:          id,
		DAGID:       dagID,
		Status:      StatusQueued,
		TriggerType: triggerType,
		ContextKey:  redis.JobContextKey(id),
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
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusSucceeded TaskStatus = "succeeded"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusSkipped   TaskStatus = "skipped"
)

type Task struct {
	ID         string     `json:"taskId"`
	JobID      string     `json:"jobId"`
	TemplateID string     `json:"templateId"`
	NodeID     string     `json:"nodeId"`
	Status     TaskStatus `json:"status"`
	ErrorMsg   string     `json:"errorMsg,omitempty"`
	BatchCount uint64     `json:"batchCount"`
	StartTime  *time.Time `json:"startTime,omitempty"`
	EndTime    *time.Time `json:"endTime,omitempty"`
}

type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

type JobLog struct {
	ID        uint64    `json:"logId"`
	JobID     string    `json:"jobId"`
	TaskID    *string   `json:"taskId,omitempty"`
	Level     LogLevel  `json:"level"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"createTime"`
}

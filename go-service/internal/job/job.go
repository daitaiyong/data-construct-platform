package job

import (
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("job not found")

type Status string

const (
	StatusQueued  Status = "queued"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type Job struct {
	ID         string    `json:"id"`
	TemplateID string    `json:"templateId"`
	Status     Status    `json:"status"`
	CreatedAt  time.Time `json:"createdAt"`
}

type Repository interface {
	Create(templateID string) Job
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

func (r *MemoryRepository) Create(templateID string) Job {
	now := time.Now().UTC()
	item := Job{ID: now.Format("20060102T150405.000000000"), TemplateID: templateID, Status: StatusQueued, CreatedAt: now}
	r.mu.Lock()
	r.jobs[item.ID] = item
	r.mu.Unlock()
	return item
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

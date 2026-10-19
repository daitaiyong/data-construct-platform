package template

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("template not found")

type Template struct {
	ID         string          `json:"templateId"`
	Name       string          `json:"name"`
	Description string         `json:"desc"`
	TargetType string          `json:"targetType"`
	BatchSize  uint32          `json:"batchSize"`
	Fields     json.RawMessage `json:"fields"`
	Creator    string          `json:"creator"`
	CreatedAt  time.Time       `json:"createTime"`
}

type Repository interface {
	List() []Template
	Create(item Template) (Template, error)
	Get(id string) (Template, error)
}

// MemoryRepository is a process-local placeholder for the future MySQL adapter.
type MemoryRepository struct {
	mu        sync.RWMutex
	templates map[string]Template
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{templates: make(map[string]Template)}
}

func (r *MemoryRepository) List() []Template {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Template, 0, len(r.templates))
	for _, item := range r.templates {
		result = append(result, item)
	}
	return result
}

func (r *MemoryRepository) Create(item Template) (Template, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return Template{}, err
	}
	item.ID = hex.EncodeToString(random)
	item.CreatedAt = time.Now().UTC()
	r.mu.Lock()
	r.templates[item.ID] = item
	r.mu.Unlock()
	return item, nil
}

func (r *MemoryRepository) Get(id string) (Template, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.templates[id]
	if !ok {
		return Template{}, ErrNotFound
	}
	return item, nil
}

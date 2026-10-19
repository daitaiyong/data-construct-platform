package template

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/id"
)

var (
	ErrNotFound = errors.New("template not found")
	ErrConflict = errors.New("template already exists")
)

type Template struct {
	ID          string     `json:"template_id,omitempty"`
	Name        string     `json:"name"`
	Description string     `json:"desc"`
	TargetType  string     `json:"target_type"`
	TargetTable string     `json:"target_table"`
	BatchSize   uint32     `json:"batch_size"`
	Fields      []Field    `json:"fields"`
	SinkConf    SinkConfig `json:"sink_conf"`
	Creator     string     `json:"creator,omitempty"`
	CreatedAt   time.Time  `json:"create_time,omitempty"`
}

type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Rule     string `json:"rule"`
	Comment  string `json:"comment"`
}

type SinkConfig struct {
	DBKey          string `json:"db_key"`
	TruncateBefore bool   `json:"truncate_before"`
}

type Repository interface {
	List(page, size int, name string) ([]Template, int)
	Create(item Template) (Template, error)
	Update(id string, item Template) (Template, error)
	Delete(id string) error
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

func (r *MemoryRepository) List(page, size int, name string) ([]Template, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if page < 1 || size < 1 {
		return []Template{}, 0
	}
	filtered := make([]Template, 0, len(r.templates))
	for _, item := range r.templates {
		if name == "" || strings.Contains(strings.ToLower(item.Name), strings.ToLower(name)) {
			filtered = append(filtered, item)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt.Equal(filtered[j].CreatedAt) {
			return filtered[i].ID < filtered[j].ID
		}
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})
	total := len(filtered)
	if total == 0 || page-1 > (total-1)/size {
		return []Template{}, total
	}
	start := (page - 1) * size
	end := start + size
	if end > total {
		end = total
	}
	return filtered[start:end], total
}

func (r *MemoryRepository) Create(item Template) (Template, error) {
	templateID, err := id.New()
	if err != nil {
		return Template{}, err
	}
	item.ID = templateID
	item.CreatedAt = time.Now().UTC()
	r.mu.Lock()
	if _, exists := r.templates[item.ID]; exists {
		r.mu.Unlock()
		return Template{}, fmt.Errorf("%w: %s", ErrConflict, item.ID)
	}
	r.templates[item.ID] = item
	r.mu.Unlock()
	return item, nil
}

func (r *MemoryRepository) Update(id string, item Template) (Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.templates[id]
	if !exists {
		return Template{}, ErrNotFound
	}
	item.ID = id
	item.CreatedAt = current.CreatedAt
	item.Creator = current.Creator
	r.templates[id] = item
	return item, nil
}

func (r *MemoryRepository) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.templates[id]; !exists {
		return ErrNotFound
	}
	delete(r.templates, id)
	return nil
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

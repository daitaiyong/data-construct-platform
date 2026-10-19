package redis

import (
	"context"
	"errors"
	"time"
)

var ErrNotConfigured = errors.New("redis adapter is not configured")

const KeyPrefix = "dc:"

const (
	JobLockTTL       = 5 * time.Minute
	TaskLockTTL      = 10 * time.Minute
	JobContextTTL    = 24 * time.Hour
	TemplateCacheTTL = time.Hour
	PreviewCacheTTL  = 10 * time.Minute
	JobMetricsTTL    = 24 * time.Hour
	SinkConcurrency  = 5
)

func TemplateKey(templateID string) string { return KeyPrefix + "template:" + templateID }

func TemplateListKey() string { return KeyPrefix + "template:list" }

func JobLockKey(jobID string) string { return KeyPrefix + "lock:job:" + jobID }

func TaskLockKey(taskID string) string { return KeyPrefix + "lock:task:" + taskID }

func JobContextKey(jobID string) string { return KeyPrefix + "job:" + jobID + ":context" }

func JobTaskCountKey(jobID string) string { return KeyPrefix + "job:" + jobID + ":task_cnt" }

func SinkLimitKey(sinkKey string) string { return KeyPrefix + "limit:sink:" + sinkKey }

func RetryQueueKey() string { return KeyPrefix + "retry:queue" }

func PreviewKey(previewRequestID string) string { return KeyPrefix + "preview:" + previewRequestID }

type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
}

type HashStore interface {
	GetAll(ctx context.Context, key string) (map[string]string, error)
	// SetFields should apply the fields and expiry together so partial updates
	// cannot leave a context hash without its retention TTL.
	SetFields(ctx context.Context, key string, fields map[string]string, ttl time.Duration) error
}

type Locker interface {
	// Lock should use an ownership token so Renew and Release only affect this lease.
	Lock(ctx context.Context, key string, ttl time.Duration) (Lease, error)
}

type Lease interface {
	Renew(ctx context.Context, ttl time.Duration) error
	Release(ctx context.Context) error
}

type Counter interface {
	Increment(ctx context.Context, key, field string, amount int64, ttl time.Duration) (int64, error)
}

type ConcurrencyLimiter interface {
	Acquire(ctx context.Context, key string, limit int, ttl time.Duration) (Lease, error)
}

type Adapter struct{}

func (Adapter) Get(context.Context, string) ([]byte, error) { return nil, ErrNotConfigured }
func (Adapter) Set(context.Context, string, []byte, time.Duration) error {
	return ErrNotConfigured
}
func (Adapter) Delete(context.Context, ...string) error { return ErrNotConfigured }
func (Adapter) GetAll(context.Context, string) (map[string]string, error) {
	return nil, ErrNotConfigured
}
func (Adapter) SetFields(context.Context, string, map[string]string, time.Duration) error {
	return ErrNotConfigured
}
func (Adapter) Increment(context.Context, string, string, int64, time.Duration) (int64, error) {
	return 0, ErrNotConfigured
}
func (Adapter) Acquire(context.Context, string, int, time.Duration) (Lease, error) {
	return nil, ErrNotConfigured
}
func (Adapter) Lock(context.Context, string, time.Duration) (Lease, error) {
	return nil, ErrNotConfigured
}

func SetTemplate(ctx context.Context, cache Cache, templateID string, value []byte) error {
	return cache.Set(ctx, TemplateKey(templateID), value, TemplateCacheTTL)
}

func AcquireJobLock(ctx context.Context, locker Locker, jobID string) (Lease, error) {
	return locker.Lock(ctx, JobLockKey(jobID), JobLockTTL)
}

func AcquireTaskLock(ctx context.Context, locker Locker, taskID string) (Lease, error) {
	return locker.Lock(ctx, TaskLockKey(taskID), TaskLockTTL)
}

func InvalidateTemplate(ctx context.Context, cache Cache, templateID string) error {
	return cache.Delete(ctx, TemplateKey(templateID))
}

func SetJobContext(ctx context.Context, store HashStore, jobID string, fields map[string]string) error {
	return store.SetFields(ctx, JobContextKey(jobID), fields, JobContextTTL)
}

func DeleteJobContext(ctx context.Context, cache Cache, jobID string) error {
	return cache.Delete(ctx, JobContextKey(jobID))
}

func IncrementJobTaskCount(ctx context.Context, counter Counter, jobID, field string, amount int64) (int64, error) {
	if field != "success_count" && field != "failure_count" {
		return 0, errors.New("task counter field must be success_count or failure_count")
	}
	if amount == 0 {
		return 0, errors.New("task counter increment must not be zero")
	}
	return counter.Increment(ctx, JobTaskCountKey(jobID), field, amount, JobMetricsTTL)
}

func AcquireSinkPermit(ctx context.Context, limiter ConcurrencyLimiter, sinkKey string) (Lease, error) {
	if sinkKey == "" {
		return nil, errors.New("sink_key must not be empty")
	}
	return limiter.Acquire(ctx, SinkLimitKey(sinkKey), SinkConcurrency, TaskLockTTL)
}

func SetPreview(ctx context.Context, cache Cache, previewRequestID string, value []byte) error {
	return cache.Set(ctx, PreviewKey(previewRequestID), value, PreviewCacheTTL)
}

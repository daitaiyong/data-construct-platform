package redis

import (
	"context"
	"testing"
	"time"
)

func TestKeysUseDataConstructPrefix(t *testing.T) {
	keys := map[string]string{
		TemplateKey("tpl1"):     "dc:template:tpl1",
		TemplateListKey():       "dc:template:list",
		JobLockKey("job1"):      "dc:lock:job:job1",
		TaskLockKey("task1"):    "dc:lock:task:task1",
		JobContextKey("job1"):   "dc:job:job1:context",
		JobTaskCountKey("job1"): "dc:job:job1:task_cnt",
		SinkLimitKey("mysql1"):  "dc:limit:sink:mysql1",
		RetryQueueKey():         "dc:retry:queue",
		PreviewKey("preview1"):  "dc:preview:preview1",
	}
	for got, want := range keys {
		if got != want {
			t.Errorf("key = %q, want %q", got, want)
		}
		if len(got) < len(KeyPrefix) || got[:len(KeyPrefix)] != KeyPrefix {
			t.Errorf("key %q does not use prefix %q", got, KeyPrefix)
		}
	}
}

func TestRedisRetentionPolicies(t *testing.T) {
	if JobLockTTL != 300*time.Second {
		t.Errorf("JobLockTTL = %s, want 300s", JobLockTTL)
	}
	if TaskLockTTL != 600*time.Second {
		t.Errorf("TaskLockTTL = %s, want 600s", TaskLockTTL)
	}
	if JobContextTTL != 24*time.Hour {
		t.Errorf("JobContextTTL = %s, want 24h", JobContextTTL)
	}
	if TemplateCacheTTL != time.Hour {
		t.Errorf("TemplateCacheTTL = %s, want 1h", TemplateCacheTTL)
	}
	if PreviewCacheTTL != 10*time.Minute {
		t.Errorf("PreviewCacheTTL = %s, want 10m", PreviewCacheTTL)
	}
}

func TestContextWritesUseHashAndTTL(t *testing.T) {
	store := &captureHashStore{}
	fields := map[string]string{"node-001.user_id_list": `["10001","10002"]`}
	if err := SetJobContext(context.Background(), store, "job1", fields); err != nil {
		t.Fatal(err)
	}
	if store.key != JobContextKey("job1") || store.ttl != JobContextTTL || store.fields["node-001.user_id_list"] != fields["node-001.user_id_list"] {
		t.Fatalf("context write = key:%q ttl:%s fields:%v", store.key, store.ttl, store.fields)
	}
}

type captureHashStore struct {
	key    string
	fields map[string]string
	ttl    time.Duration
}

func (s *captureHashStore) GetAll(context.Context, string) (map[string]string, error) {
	return nil, nil
}

func (s *captureHashStore) SetFields(_ context.Context, key string, fields map[string]string, ttl time.Duration) error {
	s.key = key
	s.fields = fields
	s.ttl = ttl
	return nil
}

package redis

import (
	"context"
	"errors"
	"time"
)

var ErrNotConfigured = errors.New("redis adapter is not configured")

type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

type Locker interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (release func() error, err error)
}

type Adapter struct{}

func (Adapter) Get(context.Context, string) ([]byte, error) { return nil, ErrNotConfigured }
func (Adapter) Set(context.Context, string, []byte, time.Duration) error {
	return ErrNotConfigured
}
func (Adapter) Lock(context.Context, string, time.Duration) (func() error, error) {
	return nil, ErrNotConfigured
}

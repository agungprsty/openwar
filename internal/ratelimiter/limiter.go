package ratelimiter

import (
	"context"
	"errors"
	"time"

	"github.com/openwar/openwar/internal/lua"
	"github.com/redis/go-redis/v9"
)

type Result struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
	// Err is non-nil when the backing store failed. Callers may combine it
	// with FailStrategy to decide between fail-open and fail-closed.
	Err error
}

type FailStrategy int

const (
	FailOpen FailStrategy = iota
	FailClosed
)

type Limiter struct {
	rdb          *redis.Client
	script       *redis.Script
	failStrategy FailStrategy

	// Bucket configuration
	capacity   float64
	refillRate float64
}

func New(rdb *redis.Client, capacity, refillRate float64, fs FailStrategy) *Limiter {
	return &Limiter{
		rdb:          rdb,
		script:       redis.NewScript(lua.TokenBucket),
		failStrategy: fs,
		capacity:     capacity,
		refillRate:   refillRate,
	}
}

// Capacity returns the bucket burst capacity.
func (l *Limiter) Capacity() int64 {
	return int64(l.capacity)
}

// Allow consumes one token for key. Returns whether the request may pass.
func (l *Limiter) Allow(ctx context.Context, key string) Result {
	now := float64(time.Now().UnixNano()) / 1e9
	res, err := l.script.Run(ctx, l.rdb, []string{key}, l.capacity, l.refillRate, now, 1).Result()
	if err != nil {
		switch l.failStrategy {
		case FailOpen:
			return Result{Allowed: true, Err: err}
		default:
			return Result{Err: err}
		}
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) < 3 {
		switch l.failStrategy {
		case FailOpen:
			return Result{Allowed: true, Err: errUnexpectedResult}
		default:
			return Result{Err: errUnexpectedResult}
		}
	}

	v, _ := toInt64(arr[0])
	remaining, _ := toInt64(arr[1])
	retry, _ := toInt64(arr[2])

	return Result{
		Allowed:    v == 1,
		Remaining:  remaining,
		RetryAfter: time.Duration(retry) * time.Second,
	}
}

var errUnexpectedResult = errors.New("token_bucket: unexpected result")

func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	case []byte:
		var i int64
		for _, b := range n {
			i = i*10 + int64(b-'0')
		}
		return i, true
	case string:
		var i int64
		for _, c := range n {
			if c < '0' || c > '9' {
				return 0, false
			}
			i = i*10 + int64(c-'0')
		}
		return i, true
	}
	return 0, false
}

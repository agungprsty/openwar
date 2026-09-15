package waitingroom

import (
	"context"
	"fmt"
	"time"

	"github.com/openwar/openwar/internal/lua"
	"github.com/openwar/openwar/internal/metrics"
	"github.com/redis/go-redis/v9"
)

type Room struct {
	rdb          *redis.Client
	admitScript  *redis.Script
	heartbeatTTL time.Duration
	admissionTTL time.Duration
}

func New(rdb *redis.Client, heartbeatTTL, admissionTTL time.Duration) *Room {
	return &Room{
		rdb:          rdb,
		admitScript:  redis.NewScript(lua.AdmitBatch),
		heartbeatTTL: heartbeatTTL,
		admissionTTL: admissionTTL,
	}
}

func queueKey(event string) string { return fmt.Sprintf("room:%s:queue", event) }
func hbKey(event, sid string) string {
	return fmt.Sprintf("room:%s:hb:%s", event, sid)
}
func admitKey(event, sid string) string {
	return fmt.Sprintf("room:%s:admitted:%s", event, sid)
}

// Join adds session to the waiting room queue. Duplicate joins are no-ops
// (ZADD NX), returning the existing position.
func (rm *Room) Join(ctx context.Context, event, sid string) (int64, error) {
	pos, err := rm.rdb.ZAddNX(ctx, queueKey(event), redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: sid,
	}).Result()
	if err != nil {
		return 0, err
	}
	rank, err := rm.rdb.ZRank(ctx, queueKey(event), sid).Result()
	if err != nil {
		return 0, err
	}
	_ = pos
	metrics.QueueDepth.WithLabelValues(event).Set(rm.depth(ctx, event))
	return rank, nil
}

func (rm *Room) depth(ctx context.Context, event string) float64 {
	return float64(rm.rdb.ZCard(ctx, queueKey(event)).Val())
}

// Status reports current position and whether the session was admitted.
func (rm *Room) Status(ctx context.Context, event, sid string) (pos int64, admitted bool, err error) {
	rank, err := rm.rdb.ZRank(ctx, queueKey(event), sid).Result()
	switch {
	case err == redis.Nil:
		return -1, false, nil // not in queue (yet or already admitted)
	case err != nil:
		return -1, false, err
	}
	adm, err := rm.rdb.Exists(ctx, admitKey(event, sid)).Result()
	if err != nil {
		return -1, false, err
	}
	return rank, adm == 1, nil
}

// Heartbeat refreshes liveness and returns position + admission state in one
// pipeline round-trip.
func (rm *Room) Heartbeat(ctx context.Context, event, sid string) (pos int64, admitted bool, err error) {
	pipe := rm.rdb.TxPipeline()
	pipe.Set(ctx, hbKey(event, sid), 1, rm.heartbeatTTL) // refresh liveness
	rank := pipe.ZRank(ctx, queueKey(event), sid)        // FIFO position
	adm := pipe.Exists(ctx, admitKey(event, sid))
	if _, err = pipe.Exec(ctx); err != nil && err != redis.Nil {
		return -1, false, err
	}
	if rank.Err() == redis.Nil {
		return -1, adm.Val() == 1, nil
	}
	return rank.Val(), adm.Val() == 1, nil
}

// Admit issues a single-use admission token for the given session (already
// validated by the worker). Returns false if the session is not live.
func (rm *Room) Admit(ctx context.Context, event, sid string) (bool, error) {
	live, err := rm.rdb.Exists(ctx, hbKey(event, sid)).Result()
	if err != nil {
		return false, err
	}
	if live == 0 {
		return false, nil
	}
	return true, rm.rdb.Set(ctx, admitKey(event, sid), "1", rm.admissionTTL).Err()
}

// HasAdmission is the admission-token check used by the checkout gate.
func (rm *Room) HasAdmission(ctx context.Context, event, sid string) (bool, error) {
	n, err := rm.rdb.Exists(ctx, admitKey(event, sid)).Result()
	return n == 1, err
}

// ConsumeAdmission verifies and single-uses the admission token.
func (rm *Room) ConsumeAdmission(ctx context.Context, event, sid string) (bool, error) {
	r := rm.rdb.Eval(ctx, lua.ConsumeAdmit, []string{admitKey(event, sid)})
	if err := r.Err(); err != nil {
		return false, err
	}
	return r.Val() == int64(1), nil
}

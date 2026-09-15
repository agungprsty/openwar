package waitingroom

import (
	"context"
	"time"

	"github.com/openwar/openwar/internal/lua"
	"github.com/openwar/openwar/internal/metrics"
	"github.com/redis/go-redis/v9"
)

// Worker admits the oldest *live* sessions at a configured rate. Zombie
// sessions (missing heartbeat) are evicted and their slot reused in-batch.
type Worker struct {
	room    *Room
	event   string
	ticker  *time.Ticker
	stop    chan struct{}
	rate    int // sessions admitted per tick
	tick    time.Duration
	onAdmit func(ctx context.Context, event, sid string)
}

func NewWorker(room *Room, event string, admittedPerTick int, interval time.Duration, onAdmit func(ctx context.Context, event, sid string)) *Worker {
	return &Worker{
		room:    room,
		event:   event,
		rate:    admittedPerTick,
		tick:    interval,
		stop:    make(chan struct{}),
		onAdmit: onAdmit,
	}
}

func (w *Worker) Start() {
	go w.loop()
}

func (w *Worker) Stop() { close(w.stop) }

func (w *Worker) loop() {
	ticker := time.NewTicker(w.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.tickOnce(context.Background())
		case <-w.stop:
			return
		}
	}
}

// tickOnce admits the next batch of live sessions.
func (w *Worker) tickOnce(ctx context.Context) {
	pipe := w.room.rdb.TxPipeline()
	pipe.Eval(ctx, lua.AdmitBatch, []string{queueKey(w.event)},
		hbKey(w.event, ""),    // prefix
		admitKey(w.event, ""), // prefix
		w.rate,
		int(w.room.admissionTTL.Seconds()),
	)
	res, err := pipe.Exec(ctx)
	if err != nil {
		return
	}

	var admitted []string
	for _, c := range res {
		if ce, ok := c.(*redis.Cmd); ok && ce.Err() == nil {
			if arr, ok := ce.Val().([]interface{}); ok {
				for _, m := range arr {
					if s, ok := m.(string); ok {
						admitted = append(admitted, s)
					}
				}
			}
		}
	}

	for _, sid := range admitted {
		metrics.AdmittedTotal.WithLabelValues(w.event).Inc()
		if w.onAdmit != nil {
			w.onAdmit(ctx, w.event, sid)
		}
	}

	metrics.QueueDepth.WithLabelValues(w.event).Set(float64(w.depth(ctx)))
}

func (w *Worker) depth(ctx context.Context) int64 {
	return w.room.rdb.ZCard(ctx, queueKey(w.event)).Val()
}

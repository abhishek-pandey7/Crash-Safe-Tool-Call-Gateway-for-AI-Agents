package gateway

import (
	"context"
	"time"
)

// pendingWrite is a state change that failed because the store was
// unreachable and is retried in the background.
type pendingWrite struct {
	desc     string
	key      string
	fn       func(context.Context) (bool, error)
	deadline time.Time
}

// pendingRetryWindow is how long a failed state write is retried.
const pendingRetryWindow = 10 * time.Minute

// persist applies a state change now, or queues it if the store is down.
// The client is answered either way: the answer reflects what actually
// happened, and the record catches up.
func (g *Gateway) persist(desc, key string, fn func(context.Context) (bool, error)) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	ok, err := fn(ctx)
	cancel()
	if err == nil {
		if !ok {
			g.log.Info("state change did not apply", "change", desc, "op", key)
		}
		return
	}
	g.log.Warn("state change failed; retrying in the background", "change", desc, "op", key, "err", err)
	select {
	case g.pending <- pendingWrite{desc: desc, key: key, fn: fn, deadline: time.Now().Add(pendingRetryWindow)}:
	default:
		// The lease will lapse and the operation will be settled as
		// unknown, which is safe.
		g.log.Error("pending queue full; dropping state change", "change", desc, "op", key)
	}
}

// Run performs background work until ctx ends: retrying failed state
// writes, settling operations whose owners vanished, and removing result
// bodies past retention.
func (g *Gateway) Run(ctx context.Context) {
	go g.retryPending(ctx)

	sweep := time.NewTicker(max(g.cfg.Lease/2, 100*time.Millisecond))
	defer sweep.Stop()
	purge := time.NewTicker(min(max(g.cfg.Retention/10, time.Second), time.Hour))
	defer purge.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sweep.C:
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			released, unknown, err := g.store.Sweep(c, g.instance)
			cancel()
			if err != nil {
				g.log.Warn("sweep failed", "err", err)
			} else if released+unknown > 0 {
				g.log.Info("settled abandoned operations", "released", released, "unknown", unknown)
			}
		case <-purge.C:
			c, cancel := context.WithTimeout(ctx, 30*time.Second)
			n, err := g.store.PurgeResults(c, g.cfg.Retention)
			cancel()
			if err != nil {
				g.log.Warn("purge failed", "err", err)
			} else if n > 0 {
				g.log.Info("removed expired result bodies", "count", n)
			}
		}
	}
}

func (g *Gateway) retryPending(ctx context.Context) {
	for {
		var w pendingWrite
		select {
		case <-ctx.Done():
			return
		case w = <-g.pending:
		}
		for delay := 100 * time.Millisecond; ; delay = min(delay*2, 5*time.Second) {
			c, cancel := context.WithTimeout(ctx, 3*time.Second)
			_, err := w.fn(c)
			cancel()
			if err == nil {
				g.log.Info("delayed state change applied", "change", w.desc, "op", w.key)
				break
			}
			if time.Now().After(w.deadline) {
				g.log.Error("giving up on state change", "change", w.desc, "op", w.key, "err", err)
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}
}

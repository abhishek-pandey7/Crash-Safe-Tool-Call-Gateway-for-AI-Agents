//go:build killpoints

// Package killpoint lets the crash suite kill the gateway at an exact step.
// It is compiled in only with the killpoints build tag; release builds get
// the no-op in killpoint_off.go.
//
// GATEWAY_KILL_AT names the point. GATEWAY_KILL_SKIP, if set, lets that many
// hits pass first. The process kills itself the way SIGKILL would: no
// deferred functions run and the operating system closes every socket.
package killpoint

import (
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
)

// Enabled reports whether kill points are compiled in.
const Enabled = true

var (
	target = os.Getenv("GATEWAY_KILL_AT")
	skip   atomic.Int64
)

func init() {
	if n, err := strconv.Atoi(os.Getenv("GATEWAY_KILL_SKIP")); err == nil {
		skip.Store(int64(n))
	}
}

// Hit kills the process if point is the configured kill point.
func Hit(point string) {
	if target == "" || point != target {
		return
	}
	if skip.Add(-1) >= 0 {
		return
	}
	fmt.Fprintf(os.Stderr, "killpoint: killing at %s\n", point)
	if p, err := os.FindProcess(os.Getpid()); err == nil {
		_ = p.Kill()
	}
	select {} // never continue past a kill point
}

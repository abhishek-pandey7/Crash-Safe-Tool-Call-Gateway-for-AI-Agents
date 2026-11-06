package faketool

import (
	"encoding/json"
	"sync"
	"time"
)

// Effect is one real side effect the fake server performed. The ledger is
// the test suite's ground truth: the gateway cannot see or change it.
type Effect struct {
	Seq       int             `json:"seq"`
	Tool      string          `json:"tool"`
	Reference string          `json:"reference,omitempty"`
	Key       string          `json:"key,omitempty"`
	Args      json.RawMessage `json:"args"`
	At        time.Time       `json:"at"`
}

// Ledger records effects in order.
type Ledger struct {
	mu      sync.Mutex
	effects []Effect
}

// Record appends an effect and returns it with its sequence number.
func (l *Ledger) Record(e Effect) Effect {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.Seq = len(l.effects) + 1
	e.At = time.Now()
	l.effects = append(l.effects, e)
	return e
}

// Effects returns a copy of every recorded effect.
func (l *Ledger) Effects() []Effect {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Effect(nil), l.effects...)
}

// Count returns how many effects match keep.
func (l *Ledger) Count(keep func(Effect) bool) int {
	n := 0
	for _, e := range l.Effects() {
		if keep(e) {
			n++
		}
	}
	return n
}

// CountReference counts effects carrying a business reference.
func (l *Ledger) CountReference(ref string) int {
	return l.Count(func(e Effect) bool { return e.Reference == ref })
}

// Reset clears the ledger.
func (l *Ledger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.effects = nil
}

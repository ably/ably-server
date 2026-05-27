// Package serial implements Ably's lexicographically-sortable
// timeserial format used for the canonical channel ordering identifier
// (`Message.Serial`, `ProtocolMessage.ChannelSerial`).
//
// Format (see DESIGN.md §8):
//
//	<timestamp>-<counter>@<seriesId>:<idx>
//	|14 digits | 3 digit |10 chars | 3 digit
//
//   - timestamp — wall-clock ms since epoch, zero-padded to 14 digits.
//   - counter   — increments when multiple serials are minted in the
//     same millisecond; resets to 000 when the timestamp advances.
//   - seriesId  — random per-process identifier; disambiguates serials
//     minted in the same millisecond on different nodes.
//   - idx       — index within an atomic publish batch (all messages
//     in one publish share `<ts>-<ctr>@<series>` and differ by idx).
//
// Lexicographic comparison of the full string matches publish order,
// which is what lets storage backends (bbolt, Postgres) use the serial
// directly as an ordered primary key.
package serial

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

const (
	timestampWidth = 14
	counterWidth   = 3
	idxWidth       = 3
	seriesIDBytes  = 5 // → 10 hex chars
	maxCounter     = 999
)

// NewSeriesID returns a fresh random per-process identifier. Panics if
// the system RNG is unavailable; we can't usefully operate without it.
func NewSeriesID() string {
	var buf [seriesIDBytes]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("serial: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(buf[:])
}

// Generator mints monotonic timeserials. One Generator instance is
// expected per channel: state (lastTs, lastCounter) is per-channel,
// while the seriesId is shared across all generators in a process.
//
// Generator is safe for concurrent use.
type Generator struct {
	seriesID string
	now      func() int64

	mu          sync.Mutex
	lastTs      int64
	lastCounter int
}

// NewGenerator constructs a Generator. If now is nil, defaults to
// time.Now().UnixMilli.
func NewGenerator(seriesID string, now func() int64) *Generator {
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	return &Generator{seriesID: seriesID, now: now}
}

// Batch mints n consecutive serials sharing a single
// `<ts>-<ctr>@<series>` prefix, with idx 0..n-1. Returns nil if n <= 0.
//
// All n serials are issued atomically — this is the unit of an "atomic
// publish" (one REST request, or one inbound MESSAGE frame carrying
// multiple messages).
func (g *Generator) Batch(n int) []string {
	if n <= 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	ts := g.now()
	if ts > g.lastTs {
		g.lastTs = ts
		g.lastCounter = 0
	} else {
		// Same ms (or clock regression). Bump counter for monotonicity.
		g.lastCounter++
		if g.lastCounter > maxCounter {
			// Counter exhausted within a single ms. Advance the
			// timestamp synthetically so monotonicity is preserved;
			// the next real-clock read will catch up.
			g.lastTs++
			g.lastCounter = 0
		}
	}

	prefix := fmt.Sprintf("%0*d-%0*d@%s", timestampWidth, g.lastTs, counterWidth, g.lastCounter, g.seriesID)
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("%s:%0*d", prefix, idxWidth, i)
	}
	return out
}

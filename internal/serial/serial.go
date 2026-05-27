// Package serial implements Ably's lexicographically-sortable
// timeserial format used for the canonical channel ordering identifier
// (`ChannelMessage.ChannelSerial`, `Message.Serial`).
//
// Two forms (see DESIGN.md §8):
//
//	channelSerial:  <timestamp>-<counter>@<seriesId>
//	                |14 digits | 3 digit |10 chars
//
//	Message.serial: <channelSerial>:<idx>
//	                                |3 digit
//
//   - timestamp — wall-clock ms since epoch, zero-padded to 14 digits.
//   - counter   — increments when multiple serials are minted in the
//     same millisecond; resets to 000 when the timestamp advances.
//   - seriesId  — random per-process identifier; disambiguates serials
//     minted in the same millisecond on different nodes.
//   - idx       — index of a Message within its containing
//     ChannelMessage (atomic publish).
//
// channelSerials are the discrete attach/resume points in a channel's
// stream — one per atomic publish. Individual Message.serials append
// the in-batch idx so each Message in a multi-message publish gets a
// distinct identifier.
//
// Lexicographic comparison of channelSerials matches publish order,
// which is what lets storage backends (bbolt, Postgres) use them
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

// Mint returns one fresh channelSerial — `<ts>-<ctr>@<series>` — for
// an atomic publish. Callers stamp individual Message serials by
// appending ":<idx>" via MessageSerial.
func (g *Generator) Mint() string {
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

	return fmt.Sprintf("%0*d-%0*d@%s", timestampWidth, g.lastTs, counterWidth, g.lastCounter, g.seriesID)
}

// MessageSerial returns the per-Message identifier for the message at
// position idx within the ChannelMessage identified by channelSerial.
//
// Format: `<channelSerial>:<idx>` with idx zero-padded to 3 digits.
func MessageSerial(channelSerial string, idx int) string {
	return fmt.Sprintf("%s:%0*d", channelSerial, idxWidth, idx)
}

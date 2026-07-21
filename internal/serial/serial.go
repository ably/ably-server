// Package serial implements Ably's lexicographically-sortable
// timeserial format used for the canonical channel ordering identifier
// (`ChannelMessage.ChannelSerial`, `Message.Serial`).
//
// Two forms (see DESIGN.md §8):
//
//		channelSerial:  <timestamp>-<counter>@<seriesId>
//		                |14 digits | 3 digit |10 chars
//
//		Message.serial: <channelSerial>:<idx>
//		                                |3 digit
//
//	  - timestamp — wall-clock ms since epoch, zero-padded to 14 digits.
//	  - counter   — increments when multiple serials are minted in the
//	    same millisecond; resets to 000 when the timestamp advances.
//	  - seriesId  — random per-process identifier; disambiguates serials
//	    minted in the same millisecond on different nodes.
//	  - idx       — index of a Message within its containing
//	    ChannelMessage (atomic publish).
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
	"strconv"
	"strings"
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

// Restore seeds the generator's monotonic state — used by persistent
// backends on startup so the first post-restart Mint produces a
// serial strictly greater than ts-ctr. Safe to call only before the
// first Mint.
func (g *Generator) Restore(ts int64, counter int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lastTs = ts
	g.lastCounter = counter
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

// ParseMessageSerial splits a Message.Serial into its component
// channelSerial and idx. The format is `<channelSerial>:<idx>`; the
// channelSerial part contains '@' but never ':' so the last ':' is
// always the boundary.
func ParseMessageSerial(s string) (channelSerial string, idx int, err error) {
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return "", 0, fmt.Errorf("serial: %q is not a Message.Serial (no ':')", s)
	}
	channelSerial = s[:i]
	idx, err = strconv.Atoi(s[i+1:])
	if err != nil {
		return "", 0, fmt.Errorf("serial: %q has non-integer idx suffix: %w", s, err)
	}
	if idx < 0 {
		return "", 0, fmt.Errorf("serial: %q has negative idx", s)
	}
	return channelSerial, idx, nil
}

// Timestamp extracts the wall-clock millisecond prefix encoded in a
// channelSerial or Message.serial (the leading 14 digits; see the
// format above). Returns an error if s is too short or the prefix is
// not numeric. Used to stamp a server-authoritative timestamp onto a
// message version from the serial the publish minted (DESIGN.md §13.1).
func Timestamp(s string) (int64, error) {
	if len(s) < timestampWidth {
		return 0, fmt.Errorf("serial: %q too short to contain a timestamp", s)
	}
	v, err := strconv.ParseInt(s[:timestampWidth], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("serial: %q has non-numeric timestamp prefix: %w", s, err)
	}
	return v, nil
}

// TimestampBounds maps an inclusive ms-since-epoch range to a
// half-open lex range over channelSerials. Useful for backends that
// implement timestamp-bounded history reads via prefix/range scans on
// the channelSerial column.
//
// A zero bound means "unbounded on that side" and is returned as an
// empty string. Otherwise:
//
//   - lower is the smallest possible channelSerial with ts == start
//     ("<start>-"). Any serial with ts >= start satisfies serial >= lower.
//   - upper is the smallest possible channelSerial with ts == end+1
//     ("<end+1>-"). Any serial with ts <= end satisfies serial < upper.
//
// So the inclusive range start <= ts <= end maps to
// (lower == "" || serial >= lower) && (upper == "" || serial < upper).
func TimestampBounds(start, end int64) (lower, upper string) {
	if start > 0 {
		lower = fmt.Sprintf("%0*d-", timestampWidth, start)
	}
	if end > 0 {
		upper = fmt.Sprintf("%0*d-", timestampWidth, end+1)
	}
	return
}

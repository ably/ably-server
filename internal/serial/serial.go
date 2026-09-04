// Package serial implements Ably's lexicographically-sortable
// timeserial format used for the canonical channel ordering identifier
// (`ChannelMessage.ChannelSerial`, `Message.Serial`).
//
// Two forms (see DESIGN.md §8):
//
//		channelSerial:  <timestamp>-<counter>@<seriesId>
//		                |14 digits | 3 digit |3-char site + 10 chars
//
//		Message.serial: <channelSerial>:<idx>
//		                                |3 digit
//
//	  - timestamp — wall-clock ms since epoch, zero-padded to 14 digits.
//	  - counter   — increments when multiple serials are minted in the
//	    same millisecond; resets to 000 when the timestamp advances.
//	  - seriesId  — identifier for the channel's series, minted once when
//	    the channel is first materialised and carried by every serial
//	    thereafter. It begins with the three-character SiteCode, which the
//	    protocol reads back off any serial, followed by random hex. A change of seriesId is what tells a client
//	    the channel's ordering restarted, so it must not change while
//	    the channel's log is intact — including across a process
//	    restart or a handover to another node.
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

// SiteCode identifies the deployment a serial was minted in. It is the first
// three characters of every seriesId, which is what makes it readable back off
// any serial (wire.Timeserial.SiteCode).
//
// Three characters is not this server's choice: the protocol reads the site
// code as the first three characters of a serial's series, and clients key
// their per-site view of an object's history by it. It lives here, with the
// serials that carry it, because that is the constraint it has to satisfy —
// a site code that did not prefix every seriesId would be unreadable from the
// serials it is supposed to identify.
//
// It must also be what the server reports in a CONNECTED frame's
// connectionDetails, because a client applying its own LiveObjects operation
// on the ACK has only that to key it by, and the echo that follows carries the
// one derived from the serial. If the two disagree the client counts its own
// operation twice (DESIGN.md §15.1).
const SiteCode = "loc"

// NewSeriesID returns a fresh per-channel identifier, prefixed with the site
// code so the site can be read back off any serial the series mints. Panics if
// the system RNG is unavailable; we can't usefully operate without it.
func NewSeriesID() string {
	var buf [seriesIDBytes]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("serial: crypto/rand failed: " + err.Error())
	}
	return SiteCode + hex.EncodeToString(buf[:])
}

// Generator mints monotonic timeserials. One Generator instance is
// expected per channel: both the monotonic state (lastTs, lastCounter)
// and the seriesId belong to the channel whose serials it mints.
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

// SplitChannelSerial breaks a channelSerial back into the three parts
// Mint assembled it from. Used by persistent backends on startup to
// carry a channel's existing series and monotonic state forward
// (DESIGN.md §8) — the series belongs to the channel, so a restart or
// a handover to another node must continue it rather than start a new
// one.
//
// A Message.serial (`<channelSerial>:<idx>`) is accepted too; the idx
// suffix is ignored.
func SplitChannelSerial(s string) (ts int64, counter int, seriesID string, err error) {
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	at := strings.IndexByte(s, '@')
	if at < 0 {
		return 0, 0, "", fmt.Errorf("serial: %q is not a channelSerial (no '@')", s)
	}
	if at != timestampWidth+1+counterWidth {
		return 0, 0, "", fmt.Errorf("serial: %q has a malformed timestamp-counter prefix", s)
	}
	ts, err = Timestamp(s)
	if err != nil {
		return 0, 0, "", err
	}
	counter, err = strconv.Atoi(s[timestampWidth+1 : at])
	if err != nil {
		return 0, 0, "", fmt.Errorf("serial: %q has a non-integer counter: %w", s, err)
	}
	return ts, counter, s[at+1:], nil
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

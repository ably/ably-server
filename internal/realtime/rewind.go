package realtime

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// rewindMode classifies a parsed rewind directive.
type rewindMode int

const (
	rewindNone rewindMode = iota
	rewindCount
	rewindDuration
)

// ParseRewind parses a rewind param from one of the following formats:
//
//	| Format | Description         | Examples         |
//	|--------|---------------------|------------------|
//	| <n>    | A positive integer  | 1, 5, 42         |
//	| <n>s   | A number of seconds | 1s, 1.23s, 46.7s |
//	| <n>m   | A number of minutes | 1m, 0.1m, 2.4m   |
//
// Empty input returns rewindNone with no error. Non-empty input that
// doesn't match any of the three formats — or has a non-positive
// numeric component — returns an error.
func ParseRewind(s string) (mode rewindMode, count int, dur time.Duration, err error) {
	if s == "" {
		return rewindNone, 0, 0, nil
	}

	switch {
	case strings.HasSuffix(s, "s"):
		secs, perr := strconv.ParseFloat(strings.TrimSuffix(s, "s"), 64)
		if perr != nil {
			return rewindNone, 0, 0, fmt.Errorf("rewind: invalid seconds value %q: %w", s, perr)
		}
		if secs <= 0 {
			return rewindNone, 0, 0, fmt.Errorf("rewind: seconds must be > 0, got %q", s)
		}
		return rewindDuration, 0, time.Duration(secs * float64(time.Second)), nil

	case strings.HasSuffix(s, "m"):
		mins, perr := strconv.ParseFloat(strings.TrimSuffix(s, "m"), 64)
		if perr != nil {
			return rewindNone, 0, 0, fmt.Errorf("rewind: invalid minutes value %q: %w", s, perr)
		}
		if mins <= 0 {
			return rewindNone, 0, 0, fmt.Errorf("rewind: minutes must be > 0, got %q", s)
		}
		return rewindDuration, 0, time.Duration(mins * float64(time.Minute)), nil

	default:
		n, perr := strconv.Atoi(s)
		if perr != nil {
			return rewindNone, 0, 0, fmt.Errorf("rewind: invalid count value %q: %w", s, perr)
		}
		if n <= 0 {
			return rewindNone, 0, 0, fmt.Errorf("rewind: count must be > 0, got %q", s)
		}
		return rewindCount, n, 0, nil
	}
}

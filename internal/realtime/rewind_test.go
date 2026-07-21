package realtime

import (
	"testing"
	"time"
)

func TestParseRewindCount(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"1", 1},
		{"5", 5},
		{"42", 42},
		{"1000", 1000},
	}
	for _, c := range cases {
		mode, count, _, err := ParseRewind(c.in)
		if err != nil {
			t.Errorf("ParseRewind(%q) error = %v", c.in, err)
			continue
		}
		if mode != rewindCount {
			t.Errorf("ParseRewind(%q) mode = %v, want rewindCount", c.in, mode)
		}
		if count != c.want {
			t.Errorf("ParseRewind(%q) count = %d, want %d", c.in, count, c.want)
		}
	}
}

func TestParseRewindDurationSeconds(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"1s", time.Second},
		{"1.5s", 1500 * time.Millisecond},
		{"46.7s", 46700 * time.Millisecond},
	}
	for _, c := range cases {
		mode, _, dur, err := ParseRewind(c.in)
		if err != nil {
			t.Errorf("ParseRewind(%q) error = %v", c.in, err)
			continue
		}
		if mode != rewindDuration {
			t.Errorf("ParseRewind(%q) mode = %v, want rewindDuration", c.in, mode)
		}
		if dur != c.want {
			t.Errorf("ParseRewind(%q) dur = %v, want %v", c.in, dur, c.want)
		}
	}
}

func TestParseRewindDurationMinutes(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"1m", time.Minute},
		{"0.1m", 6 * time.Second},
		{"2.4m", 144 * time.Second},
	}
	for _, c := range cases {
		mode, _, dur, err := ParseRewind(c.in)
		if err != nil {
			t.Errorf("ParseRewind(%q) error = %v", c.in, err)
			continue
		}
		if mode != rewindDuration {
			t.Errorf("ParseRewind(%q) mode = %v, want rewindDuration", c.in, mode)
		}
		if dur != c.want {
			t.Errorf("ParseRewind(%q) dur = %v, want %v", c.in, dur, c.want)
		}
	}
}

func TestParseRewindEmpty(t *testing.T) {
	mode, _, _, err := ParseRewind("")
	if err != nil {
		t.Fatalf("ParseRewind(\"\") error = %v, want nil", err)
	}
	if mode != rewindNone {
		t.Errorf("ParseRewind(\"\") mode = %v, want rewindNone", mode)
	}
}

func TestParseRewindRejectsInvalid(t *testing.T) {
	bad := []string{
		"0",      // count must be > 0
		"-1",     // negative count
		"abc",    // not a number
		"0s",     // zero seconds
		"-1s",    // negative seconds
		"abcs",   // unparseable seconds
		"0m",     // zero minutes
		"1.2.3s", // malformed float
		"5h",     // unsupported unit
		"1ms",    // ms not in spec
	}
	for _, in := range bad {
		if _, _, _, err := ParseRewind(in); err == nil {
			t.Errorf("ParseRewind(%q) expected error, got nil", in)
		}
	}
}

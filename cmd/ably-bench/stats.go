package main

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// Payload wire format. We embed everything the receiver needs to verify
// delivery and measure latency directly in the message data, then pad to
// the requested size. Because the same process both publishes and
// subscribes, the publish timestamp is taken from the same clock the
// receiver reads — so latency is skew-free without any clock sync.
//
//	"<pubID>:<seq>:<publishUnixNano>:<padding...>"
//
// pubID is unique per publisher connection; seq increments per publisher
// starting at 0, so a receiver can check a publisher's stream for loss,
// duplication, and reordering with simple per-publisher counters.

func encodePayload(pubID int, seq int64, tNano int64, size int) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(pubID))
	b.WriteByte(':')
	b.WriteString(strconv.FormatInt(seq, 10))
	b.WriteByte(':')
	b.WriteString(strconv.FormatInt(tNano, 10))
	b.WriteByte(':')
	if pad := size - b.Len(); pad > 0 {
		b.WriteString(strings.Repeat("x", pad))
	}
	return b.String()
}

func decodePayload(s string) (pubID int, seq, tNano int64, ok bool) {
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return 0, 0, 0, false
	}
	j := strings.IndexByte(s[i+1:], ':')
	if j < 0 {
		return 0, 0, 0, false
	}
	j += i + 1
	k := strings.IndexByte(s[j+1:], ':')
	if k < 0 {
		return 0, 0, 0, false
	}
	k += j + 1

	var err error
	if pubID, err = strconv.Atoi(s[:i]); err != nil {
		return 0, 0, 0, false
	}
	if seq, err = strconv.ParseInt(s[i+1:j], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if tNano, err = strconv.ParseInt(s[j+1:k], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	return pubID, seq, tNano, true
}

// histogram is a fixed-resolution latency histogram: linear buckets of
// resUS microseconds up to len(buckets)*resUS, with everything above
// folded into an overflow count. 100µs resolution to ~6s keeps it small
// (~60k int64 = ~480KB) while staying well under a millisecond of error
// for the percentiles we report. The exact max is tracked separately so
// the long tail isn't lost to the overflow bucket.
type histogram struct {
	buckets  []int64
	resUS    int64
	count    int64
	overflow int64
	maxUS    int64
}

func newHistogram() *histogram {
	const resUS = 100
	const maxUS = 6 * time.Second / time.Microsecond
	return &histogram{
		buckets: make([]int64, int(int64(maxUS)/resUS)),
		resUS:   resUS,
	}
}

func (h *histogram) record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	h.count++
	h.maxUS = max(h.maxUS, us)
	idx := us / h.resUS
	if idx >= int64(len(h.buckets)) {
		h.overflow++
		return
	}
	h.buckets[idx]++
}

func (h *histogram) merge(o *histogram) {
	for i, c := range o.buckets {
		h.buckets[i] += c
	}
	h.count += o.count
	h.overflow += o.overflow
	if o.maxUS > h.maxUS {
		h.maxUS = o.maxUS
	}
}

// percentile returns the p-th percentile (0..100) using the bucket upper
// edge. Samples that landed in overflow are reported as the exact max.
func (h *histogram) percentile(p float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	target := max(int64(math.Ceil(p/100*float64(h.count))), 1)
	var cum int64
	for i, c := range h.buckets {
		cum += c
		if cum >= target {
			return time.Duration(int64(i+1)*h.resUS) * time.Microsecond
		}
	}
	return time.Duration(h.maxUS) * time.Microsecond
}

func (h *histogram) max() time.Duration {
	return time.Duration(h.maxUS) * time.Microsecond
}

// pubSeqState tracks one publisher's stream as seen by one subscriber.
// A correct stream is the contiguous sequence 0,1,2,…: expectNext walks
// it, forwardGaps counts skipped seqs (loss or not-yet-arrived), and
// backwards counts seqs at or below what we expected (duplicate or
// reorder). All access is from a single channel-subscription goroutine,
// which ably-go serialises, so no locking is needed.
type pubSeqState struct {
	expectNext  int64
	recv        int64
	forwardGaps int64
	backwards   int64
}

func (s *pubSeqState) observe(seq int64) {
	s.recv++
	switch {
	case seq == s.expectNext:
		s.expectNext++
	case seq > s.expectNext:
		s.forwardGaps += seq - s.expectNext
		s.expectNext = seq + 1
	default:
		s.backwards++
	}
}

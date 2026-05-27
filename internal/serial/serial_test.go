package serial

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// fixedClock returns a clock func that always reports ts.
func fixedClock(ts int64) func() int64 {
	return func() int64 { return ts }
}

// stepClock returns a clock func that advances by 1 ms on every call,
// starting at start.
func stepClock(start int64) func() int64 {
	var ts atomic.Int64
	ts.Store(start - 1)
	return func() int64 { return ts.Add(1) }
}

func TestNewSeriesIDLength(t *testing.T) {
	id := NewSeriesID()
	if len(id) != seriesIDBytes*2 {
		t.Errorf("len = %d, want %d", len(id), seriesIDBytes*2)
	}
}

func TestNewSeriesIDIsRandom(t *testing.T) {
	a := NewSeriesID()
	b := NewSeriesID()
	if a == b {
		t.Errorf("two NewSeriesID calls returned the same value %q", a)
	}
}

func TestBatchSingleMessageFormat(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1726585978590))
	out := g.Batch(1)
	if len(out) != 1 {
		t.Fatalf("len = %d, want 1", len(out))
	}
	want := "01726585978590-000@abcdefghij:000"
	if out[0] != want {
		t.Errorf("got %q, want %q", out[0], want)
	}
}

func TestBatchAtomicShareTimestampCounterSeries(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1726585978590))
	out := g.Batch(3)
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3", len(out))
	}
	prefix := "01726585978590-000@abcdefghij"
	for i, s := range out {
		want := prefix + ":" + []string{"000", "001", "002"}[i]
		if s != want {
			t.Errorf("[%d] = %q, want %q", i, s, want)
		}
	}
}

func TestBatchSameMillisecondAdvancesCounter(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1726585978590))
	first := g.Batch(1)[0]
	second := g.Batch(1)[0]
	if !strings.HasSuffix(first, "-000@abcdefghij:000") {
		t.Errorf("first = %q, want suffix -000@abcdefghij:000", first)
	}
	if !strings.HasSuffix(second, "-001@abcdefghij:000") {
		t.Errorf("second = %q, want suffix -001@abcdefghij:000", second)
	}
}

func TestBatchNewMillisecondResetsCounter(t *testing.T) {
	g := NewGenerator("abcdefghij", stepClock(1000))
	a := g.Batch(1)[0]
	b := g.Batch(1)[0]
	if !strings.HasSuffix(a, "-000@abcdefghij:000") {
		t.Errorf("a = %q, want counter 000", a)
	}
	if !strings.HasSuffix(b, "-000@abcdefghij:000") {
		t.Errorf("b = %q, want counter 000", b)
	}
	if a >= b {
		t.Errorf("ordering broken: a=%q b=%q", a, b)
	}
}

func TestBatchCounterCarriesWhenExhausted(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1000))
	for range maxCounter + 1 {
		g.Batch(1)
	}
	// One more should overflow the counter and advance the synthetic
	// timestamp.
	over := g.Batch(1)[0]
	want := "00000000001001-000@abcdefghij:000"
	if over != want {
		t.Errorf("got %q, want %q", over, want)
	}
}

func TestBatchClockRegressionStaysMonotonic(t *testing.T) {
	ts := int64(2000)
	g := NewGenerator("abcdefghij", func() int64 { return ts })
	a := g.Batch(1)[0]
	ts = 1000 // clock went backwards
	b := g.Batch(1)[0]
	if a >= b {
		t.Errorf("monotonicity broken under clock regression: a=%q b=%q", a, b)
	}
}

func TestBatchLexicographicOrderingMatchesPublishOrder(t *testing.T) {
	g := NewGenerator("abcdefghij", stepClock(5000))
	prev := ""
	for range 50 {
		got := g.Batch(1)[0]
		if got <= prev {
			t.Fatalf("not monotonic: %q <= %q", got, prev)
		}
		prev = got
	}
}

func TestBatchNegativeOrZero(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1000))
	if out := g.Batch(0); out != nil {
		t.Errorf("Batch(0) = %v, want nil", out)
	}
	if out := g.Batch(-3); out != nil {
		t.Errorf("Batch(-3) = %v, want nil", out)
	}
}

func TestBatchIsConcurrentSafe(t *testing.T) {
	g := NewGenerator("abcdefghij", stepClock(10000))
	const workers = 20
	const perWorker = 50

	seen := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range perWorker {
				out := g.Batch(1)
				if _, dup := seen.LoadOrStore(out[0], struct{}{}); dup {
					t.Errorf("duplicate serial: %q", out[0])
					return
				}
			}
		}()
	}
	wg.Wait()
}

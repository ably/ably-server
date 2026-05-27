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

func TestMintFormat(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1726585978590))
	got := g.Mint()
	want := "01726585978590-000@abcdefghij"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestMintSameMillisecondAdvancesCounter(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1726585978590))
	first := g.Mint()
	second := g.Mint()
	if !strings.HasSuffix(first, "-000@abcdefghij") {
		t.Errorf("first = %q, want suffix -000@abcdefghij", first)
	}
	if !strings.HasSuffix(second, "-001@abcdefghij") {
		t.Errorf("second = %q, want suffix -001@abcdefghij", second)
	}
}

func TestMintNewMillisecondResetsCounter(t *testing.T) {
	g := NewGenerator("abcdefghij", stepClock(1000))
	a := g.Mint()
	b := g.Mint()
	if !strings.HasSuffix(a, "-000@abcdefghij") {
		t.Errorf("a = %q, want counter 000", a)
	}
	if !strings.HasSuffix(b, "-000@abcdefghij") {
		t.Errorf("b = %q, want counter 000", b)
	}
	if a >= b {
		t.Errorf("ordering broken: a=%q b=%q", a, b)
	}
}

func TestMintCounterCarriesWhenExhausted(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1000))
	for range maxCounter + 1 {
		g.Mint()
	}
	// One more should overflow the counter and advance the synthetic
	// timestamp.
	over := g.Mint()
	want := "00000000001001-000@abcdefghij"
	if over != want {
		t.Errorf("got %q, want %q", over, want)
	}
}

func TestMintClockRegressionStaysMonotonic(t *testing.T) {
	ts := int64(2000)
	g := NewGenerator("abcdefghij", func() int64 { return ts })
	a := g.Mint()
	ts = 1000 // clock went backwards
	b := g.Mint()
	if a >= b {
		t.Errorf("monotonicity broken under clock regression: a=%q b=%q", a, b)
	}
}

func TestMintLexicographicOrderingMatchesPublishOrder(t *testing.T) {
	g := NewGenerator("abcdefghij", stepClock(5000))
	prev := ""
	for range 50 {
		got := g.Mint()
		if got <= prev {
			t.Fatalf("not monotonic: %q <= %q", got, prev)
		}
		prev = got
	}
}

func TestMintIsConcurrentSafe(t *testing.T) {
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
				got := g.Mint()
				if _, dup := seen.LoadOrStore(got, struct{}{}); dup {
					t.Errorf("duplicate channelSerial: %q", got)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestMessageSerial(t *testing.T) {
	cs := "01726585978590-000@abcdefghij"
	cases := []struct {
		idx  int
		want string
	}{
		{0, "01726585978590-000@abcdefghij:000"},
		{5, "01726585978590-000@abcdefghij:005"},
		{42, "01726585978590-000@abcdefghij:042"},
		{999, "01726585978590-000@abcdefghij:999"},
	}
	for _, tc := range cases {
		got := MessageSerial(cs, tc.idx)
		if got != tc.want {
			t.Errorf("MessageSerial(%q, %d) = %q, want %q", cs, tc.idx, got, tc.want)
		}
	}
}

func TestRestoreMakesNextMintStrictlyGreater(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(1000))
	g.Restore(2000, 5)

	// Clock reports 1000 (< 2000), so Mint stays at restored ts and
	// bumps counter to 6.
	got := g.Mint()
	want := "00000000002000-006@abcdefghij"
	if got != want {
		t.Errorf("after Restore at lower clock: got %q, want %q", got, want)
	}
}

func TestRestoreYieldsToAdvancingClock(t *testing.T) {
	g := NewGenerator("abcdefghij", fixedClock(3000))
	g.Restore(2000, 5)

	// Clock 3000 > restored ts 2000, so first Mint resets counter at
	// the new ts.
	got := g.Mint()
	want := "00000000003000-000@abcdefghij"
	if got != want {
		t.Errorf("after Restore with advancing clock: got %q, want %q", got, want)
	}
}

func TestMessageSerialOrdersWithinBatch(t *testing.T) {
	cs := "01726585978590-000@abcdefghij"
	prev := MessageSerial(cs, 0)
	for i := 1; i < 10; i++ {
		got := MessageSerial(cs, i)
		if got <= prev {
			t.Errorf("not monotonic: %q <= %q", got, prev)
		}
		prev = got
	}
}

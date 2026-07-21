package core

import (
	"context"
	"sync"
	"testing"

	"github.com/ably/ably-server/internal/storage/memory"
)

func newTestManager() *Manager {
	return NewManager(memory.New(memory.Options{}))
}

func mustGetChannel(t *testing.T, m *Manager, name string) *Channel {
	t.Helper()
	ch, err := m.GetChannel(context.Background(), name)
	if err != nil {
		t.Fatalf("GetChannel(%q): %v", name, err)
	}
	return ch
}

func TestManagerGetChannelCreates(t *testing.T) {
	m := newTestManager()
	c := mustGetChannel(t, m, "foo")
	if c == nil {
		t.Fatal("GetChannel returned nil")
	}
	if c.Name() != "foo" {
		t.Fatalf("Name = %q, want %q", c.Name(), "foo")
	}
}

func TestManagerGetChannelIsIdempotent(t *testing.T) {
	m := newTestManager()
	a := mustGetChannel(t, m, "foo")
	b := mustGetChannel(t, m, "foo")
	if a != b {
		t.Fatal("GetChannel returned different instances for the same name")
	}
}

func TestManagerGetChannelDistinctNames(t *testing.T) {
	m := newTestManager()
	a := mustGetChannel(t, m, "foo")
	b := mustGetChannel(t, m, "bar")
	if a == b {
		t.Fatal("GetChannel returned the same instance for different names")
	}
}

func TestManagerGetChannelConcurrent(t *testing.T) {
	m := newTestManager()
	const goroutines = 50
	results := make(chan *Channel, goroutines)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			ch, err := m.GetChannel(context.Background(), "foo")
			if err != nil {
				t.Errorf("GetChannel: %v", err)
				return
			}
			results <- ch
		}()
	}
	wg.Wait()
	close(results)

	var first *Channel
	for ch := range results {
		if first == nil {
			first = ch
		} else if first != ch {
			t.Fatal("concurrent GetChannel returned different instances")
		}
	}
}

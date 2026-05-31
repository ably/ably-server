package core

import (
	"sync"
	"testing"
)

func TestManagerGetChannelCreates(t *testing.T) {
	m := NewManager()
	c := m.GetChannel("foo")
	if c == nil {
		t.Fatal("GetChannel returned nil")
	}
	if c.Name() != "foo" {
		t.Fatalf("Name = %q, want %q", c.Name(), "foo")
	}
}

func TestManagerGetChannelIsIdempotent(t *testing.T) {
	m := NewManager()
	a := m.GetChannel("foo")
	b := m.GetChannel("foo")
	if a != b {
		t.Fatal("GetChannel returned different instances for the same name")
	}
}

func TestManagerGetChannelDistinctNames(t *testing.T) {
	m := NewManager()
	a := m.GetChannel("foo")
	b := m.GetChannel("bar")
	if a == b {
		t.Fatal("GetChannel returned the same instance for different names")
	}
}

func TestManagerGetChannelConcurrent(t *testing.T) {
	m := NewManager()
	const goroutines = 50
	results := make(chan *Channel, goroutines)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			results <- m.GetChannel("foo")
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

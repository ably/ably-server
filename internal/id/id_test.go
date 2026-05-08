package id

import "testing"

func TestNewConnectionID(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for range 1000 {
		id := NewConnectionID()
		if len(id) != 12 {
			t.Fatalf("connection ID length = %d, want 12 (got %q)", len(id), id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate connection ID: %q", id)
		}
		seen[id] = struct{}{}
	}
}

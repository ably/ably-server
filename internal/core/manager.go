package core

import (
	"sync"

	"github.com/ably/ably-server/internal/storage"
)

// Manager owns the set of active Channels in this process. Each
// Channel is backed by a per-name facet of the shared storage, which
// is the sole authority for channelSerial minting and idempotency
// (DESIGN.md §6, §8).
type Manager struct {
	store storage.Storage

	mu       sync.Mutex
	channels map[string]*Channel
}

// NewManager constructs an empty Manager backed by store.
func NewManager(store storage.Storage) *Manager {
	return &Manager{
		store:    store,
		channels: make(map[string]*Channel),
	}
}

// GetChannel returns the Channel for name, creating it if necessary.
// Concurrent calls for the same name observe the same instance.
func (m *Manager) GetChannel(name string) *Channel {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.channels[name]; ok {
		return ch
	}
	ch := newChannel(name, m.store.Channel(name))
	m.channels[name] = ch
	return ch
}

package core

import (
	"sync"

	"github.com/ably/ably-server/internal/storage"
)

// Manager owns the set of active Channels in this process. It pairs
// each Channel with its storage.ChannelStore at creation time — the
// Channel is passed to the storage as the Appender, so persisted cms
// can flow back into the live list. Lifecycle (which channels exist,
// returning the same instance for the same name) is Manager's only
// job; persistence and pub/sub mechanics live in storage.
type Manager struct {
	store storage.Storage

	mu       sync.Mutex
	channels map[string]*Channel
}

// NewManager constructs a Manager backed by store.
func NewManager(store storage.Storage) *Manager {
	return &Manager{
		store:    store,
		channels: make(map[string]*Channel),
	}
}

// GetChannel returns the Channel for name, creating it if necessary
// and registering it with the storage backend as the Appender for
// that channel's stream of persisted cms. Concurrent calls for the
// same name observe the same instance.
func (m *Manager) GetChannel(name string) *Channel {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.channels[name]; ok {
		return ch
	}
	ch := newChannel(name, nil)
	ch.store = m.store.Channel(name, ch)
	m.channels[name] = ch
	return ch
}

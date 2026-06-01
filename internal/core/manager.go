package core

import (
	"context"
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
//
// On first creation the storage backend will call Channel.Initialize
// with the channel's watermark serial before this returns, so the
// returned Channel is ready for Attach. The Manager's mu is released
// across the storage call to avoid holding it through any I/O.
func (m *Manager) GetChannel(ctx context.Context, name string) (*Channel, error) {
	m.mu.Lock()
	if ch, ok := m.channels[name]; ok {
		m.mu.Unlock()
		return ch, nil
	}
	ch := newChannel(name)
	m.channels[name] = ch
	m.mu.Unlock()

	store, err := m.store.Channel(ctx, name, ch)
	if err != nil {
		// Roll back: the half-constructed channel never finished init.
		// Drop it so a later GetChannel can retry from scratch.
		m.mu.Lock()
		if cur, ok := m.channels[name]; ok && cur == ch {
			delete(m.channels, name)
		}
		m.mu.Unlock()
		return nil, err
	}
	ch.store = store
	return ch, nil
}

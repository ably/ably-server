package core

import "sync"

// Manager owns the set of active Channels in this process.
type Manager struct {
	mu       sync.Mutex
	channels map[string]*Channel
}

// NewManager constructs an empty Manager.
func NewManager() *Manager {
	return &Manager{channels: make(map[string]*Channel)}
}

// GetChannel returns the Channel for name, creating it if necessary.
// Concurrent calls for the same name observe the same instance.
func (m *Manager) GetChannel(name string) *Channel {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.channels[name]; ok {
		return ch
	}
	ch := newChannel(name)
	m.channels[name] = ch
	return ch
}

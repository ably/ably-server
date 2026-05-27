package core

import (
	"sync"

	"github.com/ably/ably-server/internal/serial"
)

// Manager owns the set of active Channels in this process, plus the
// per-process seriesId used by every channel's serial generator.
type Manager struct {
	seriesID string
	now      func() int64

	mu       sync.Mutex
	channels map[string]*Channel
}

// NewManager constructs an empty Manager with a freshly-minted
// seriesId and the real-time clock.
func NewManager() *Manager {
	return newManager(serial.NewSeriesID(), nil)
}

// NewManagerWithClock is NewManager with an injected clock — used by
// tests that need deterministic timeserials.
func NewManagerWithClock(now func() int64) *Manager {
	return newManager(serial.NewSeriesID(), now)
}

func newManager(seriesID string, now func() int64) *Manager {
	return &Manager{
		seriesID: seriesID,
		now:      now,
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
	ch := newChannel(name, serial.NewGenerator(m.seriesID, m.now))
	m.channels[name] = ch
	return ch
}

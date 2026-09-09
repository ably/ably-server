package handles

import (
	"testing"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/storage/memory"

	protoapp "github.com/ably/server-protocol/go/app"
	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/conf"
	"github.com/ably/server-protocol/go/scope"
	"github.com/ably/server-protocol/go/wire"
)

// testAppScope is the app every channel a test makes belongs to.
var testAppScope = scope.New(scope.App, "ably-server")

// newTestChannels returns a channel manager over a store nothing else is
// using, so that each test starts with an empty server.
func newTestChannels(t *testing.T) (*ChannelManager, *core.Manager) {
	t.Helper()

	store := core.NewManager(memory.New(memory.Options{}))
	m := NewChannelManager(store, conf.Default(), protoapp.NewNamespaceMap(), logging.Default())
	t.Cleanup(m.Close)
	return m, store
}

// heldChannel is one channel as the module's manager hands it out: the
// server's own liveChannel, started, behind the cache the module initialised
// for it. Holding one is what keeps the channel from being collected, which is
// why a test keeps this rather than the pieces it reads through.
type heldChannel struct {
	ref channel.Ref

	// stored is the same channel as this server's storage holds it, which is
	// how a test arranges what the channel already has on it.
	stored *core.Channel
}

func (c heldChannel) live() *liveChannel              { return c.ref.Get().Channel.(*liveChannel) }
func (c heldChannel) messages() *channel.MessageCache { return c.ref.Get().MessageCache() }
func (c heldChannel) presence() channel.Presence      { return c.ref.Get().Presence() }

// newTestChannel is one channel of a server nothing else is using, made the
// way a served request makes one.
func newTestChannel(t *testing.T, name string) heldChannel {
	t.Helper()

	m, store := newTestChannels(t)
	return getTestChannel(t, m, store, name)
}

// newTestChannelWithHistory is a channel whose messages were all published
// before it came live, so its cache holds none of them and every read of them
// has to reach into storage. That is the case a cache holding only what it has
// seen cannot answer, and the one worth arranging deliberately: a channel fed
// while it is live races the tail that feeds it.
func newTestChannelWithHistory(t *testing.T, name string, bodies ...string) (heldChannel, []*wire.Timeserial) {
	t.Helper()

	m, store := newTestChannels(t)
	stored, err := store.GetChannel(t.Context(), name)
	if err != nil {
		t.Fatalf("GetChannel(%q): %s", name, err)
	}
	serials := publishTestMessages(t, stored, bodies...)
	return getTestChannel(t, m, store, name), serials
}

// getTestChannel borrows one channel from an existing manager, so that a test
// about two attachments sharing a channel is asking the manager the same
// question twice.
func getTestChannel(t *testing.T, m *ChannelManager, store *core.Manager, name string) heldChannel {
	t.Helper()

	stored, err := store.GetChannel(t.Context(), name)
	if err != nil {
		t.Fatalf("GetChannel(%q): %s", name, err)
	}
	spec, errInfo := channel.ParseSpec(name, testAppScope)
	if errInfo != nil {
		t.Fatalf("ParseSpec(%q): %s", name, errInfo)
	}
	ref, errInfo := m.Public().GetChannel(t.Context(), spec)
	if errInfo != nil {
		t.Fatalf("borrowing the channel %q: %s", name, errInfo)
	}
	return heldChannel{ref: ref, stored: stored}
}

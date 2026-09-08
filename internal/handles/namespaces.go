package handles

import (
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"

	protoapp "github.com/ably/server-protocol/go/app"
	"github.com/ably/server-protocol/go/live"
	"github.com/ably/server-protocol/go/resource"
	"github.com/ably/server-protocol/go/scope"
	"github.com/ably/server-protocol/go/wire"
)

// resourceCapabilities is the key's capability in the protocol code's terms,
// defaulting to this app's scope for an entry that names none.
func resourceCapabilities(k auth.APIKey, appScope scope.ID) (resource.Capabilities, error) {
	capabilities, err := resource.ParseCapabilities(k.CapabilityString(), appScope)
	if err != nil {
		return nil, fmt.Errorf("key %s: %w", k.Name(), err)
	}
	return capabilities, nil
}

// namespaces is the app's namespaces as the protocol code reads them: a live
// map, so that a namespace added or changed while the server runs reaches the
// channels already resolved against it (DESIGN.md §9.1).
//
// Alongside the map it keeps the namespaces ordered by specificity, which is
// what resolving a channel's namespace scans: more than one namespace can
// match a name, and the most specific of them is the one that applies. The
// order depends only on the namespaces themselves, so it is rebuilt when the
// set changes rather than per channel, of which there may be very many.
type namespaces struct {
	// mu guards items and the rebuild of index, so that two Set calls cannot
	// interleave into a map and an index that disagree. Reads take neither:
	// items is replaced wholesale and index is atomic.
	mu    sync.Mutex
	items atomic.Pointer[map[string]*live.Value[*wire.Namespace]]
	index atomic.Pointer[protoapp.NamespaceIndex]

	// added notifies that a namespace appeared and updated that one changed or
	// went away. A channel selects on both, because any namespace may be the
	// one that matches it: an addition matters as much as a change.
	added   *live.Value[string]
	updated *live.Value[struct{}]

	// loaded is closed as soon as the map holds the app's namespaces, which is
	// before the listener opens. A reader waiting to find out whether a
	// namespace it cannot find is absent or merely not read yet never waits.
	loaded chan struct{}
}

var _ protoapp.NamespaceMap = (*namespaces)(nil)

// newNamespaces returns the configured namespaces, already loaded.
func newNamespaces(configured []config.Namespace) *namespaces {
	n := &namespaces{
		added:   live.NewValue(""),
		updated: live.NewValue(struct{}{}),
		loaded:  make(chan struct{}),
	}
	n.items.Store(&map[string]*live.Value[*wire.Namespace]{})
	n.index.Store(protoapp.NewNamespaceIndex(n.All()))
	n.Set(configured)
	close(n.loaded)
	return n
}

// Set replaces the app's namespaces with the ones configured, notifying
// whatever is watching for what actually changed: an id that was not there
// before notifies added, and one whose settings changed or that is no longer
// configured notifies updated. A call that changes nothing notifies nothing,
// so a re-read of an unedited directory costs a watcher no work.
//
// Several namespaces added at once notify added once, naming one of them. What
// a watcher does with the notification is resolve its own channel again, and
// which id it carries makes no difference to that.
//
// The index is rebuilt before either notification fires, because a watcher
// re-resolves by reading the notifications and then scanning the index: were
// the order the other way round, a channel could read the new notification,
// scan the old index, and never be told again.
func (n *namespaces) Set(configured []config.Namespace) {
	n.mu.Lock()
	defer n.mu.Unlock()

	current := *n.items.Load()
	next := make(map[string]*live.Value[*wire.Namespace], len(configured))
	var addedID string
	changed := false

	for _, c := range configured {
		ns := &wire.Namespace{
			Id:              c.ID,
			Mode:            c.Mode,
			Persisted:       c.Persisted,
			MutableMessages: c.MutableMessages,
			PushEnabled:     c.PushEnabled,
		}
		value, ok := current[c.ID]
		switch {
		case !ok:
			next[c.ID] = live.NewValue(ns)
			addedID = c.ID
		case !sameNamespace(value.Get(), ns):
			value.Set(ns)
			next[c.ID] = value
			changed = true
		default:
			next[c.ID] = value
		}
	}
	// A namespace that is no longer configured leaves the map; a channel that
	// resolved to it has to resolve again, which is what updated tells it.
	for id := range current {
		if _, ok := next[id]; !ok {
			changed = true
			break
		}
	}

	if addedID == "" && !changed {
		return
	}

	n.items.Store(&next)
	n.index.Store(protoapp.NewNamespaceIndex(n.All()))

	if changed {
		n.updated.Set(struct{}{})
	}
	if addedID != "" {
		n.added.Set(addedID)
	}
}

// sameNamespace reports whether two namespaces carry the same settings. Only
// the fields this server configures are compared, since they are the only ones
// it ever sets.
func sameNamespace(a, b *wire.Namespace) bool {
	return a.GetId() == b.GetId() &&
		a.GetMode() == b.GetMode() &&
		a.GetPersisted() == b.GetPersisted() &&
		a.GetMutableMessages() == b.GetMutableMessages() &&
		a.GetPushEnabled() == b.GetPushEnabled()
}

func (n *namespaces) Get(id string) (*live.State[*wire.Namespace], bool) {
	value, ok := (*n.items.Load())[id]
	if !ok {
		return nil, false
	}
	return value.State(), true
}

func (n *namespaces) Added() *live.State[string]     { return n.added.State() }
func (n *namespaces) Updated() *live.State[struct{}] { return n.updated.State() }
func (n *namespaces) Loaded() <-chan struct{}        { return n.loaded }
func (n *namespaces) Size() int                      { return len(*n.items.Load()) }

func (n *namespaces) Index() *protoapp.NamespaceIndex { return n.index.Load() }

// All iterates the namespaces in id order, so that an index built from it
// orders two namespaces claiming the same expression the same way every time.
func (n *namespaces) All() iter.Seq[*live.State[*wire.Namespace]] {
	items := *n.items.Load()
	return func(yield func(*live.State[*wire.Namespace]) bool) {
		for _, id := range slices.Sorted(maps.Keys(items)) {
			if !yield(items[id].State()) {
				return
			}
		}
	}
}

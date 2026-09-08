package handles

import (
	"testing"

	"github.com/ably/ably-server/internal/config"
)

// TestNamespacesNotifyWhatChanged proves the map tells a watcher apart what it
// needs to act on from what it does not: an addition notifies added, a change
// or a removal notifies updated, and re-applying the same set notifies
// neither.
func TestNamespacesNotifyWhatChanged(t *testing.T) {
	n := newNamespaces([]config.Namespace{{ID: "chat", Persisted: true}})

	// A watcher takes the notification channels the way ResolveNamespace does,
	// before reading anything.
	added, updated := n.Added().Notify, n.Updated().Notify

	// Re-applying the same set changes nothing, so a re-read of an unedited
	// directory costs an attached channel no work.
	n.Set([]config.Namespace{{ID: "chat", Persisted: true}})
	if isClosed(added) || isClosed(updated) {
		t.Error("an unchanged set notified a watcher")
	}

	// A new namespace notifies added: any namespace may be the one that
	// matches a given channel, so an addition matters as much as a change.
	n.Set([]config.Namespace{{ID: "chat", Persisted: true}, {ID: "*:edits", Mode: "matcher"}})
	if !isClosed(added) {
		t.Error("an added namespace did not notify")
	}
	if n.Size() != 2 {
		t.Errorf("Size() = %d, want 2", n.Size())
	}

	// A changed namespace notifies updated, and the index it is resolved
	// through is rebuilt before the notification fires.
	added, updated = n.Added().Notify, n.Updated().Notify
	n.Set([]config.Namespace{{ID: "chat", Persisted: true, MutableMessages: true}, {ID: "*:edits", Mode: "matcher"}})
	if !isClosed(updated) {
		t.Error("a changed namespace did not notify")
	}
	if isClosed(added) {
		t.Error("a changed namespace was reported as added")
	}
	if got := n.Index().MostSpecific("chat:room"); !got.GetMutableMessages() {
		t.Error("the index still resolves chat:room to the old settings")
	}

	// A namespace that is no longer configured notifies updated too: a channel
	// that resolved to it has to resolve again.
	added, updated = n.Added().Notify, n.Updated().Notify
	n.Set([]config.Namespace{{ID: "chat", Persisted: true, MutableMessages: true}})
	if !isClosed(updated) {
		t.Error("a removed namespace did not notify")
	}
	if isClosed(added) {
		t.Error("a removed namespace was reported as added")
	}
	if _, ok := n.Get("*:edits"); ok {
		t.Error("a removed namespace is still in the map")
	}
}

// A held state keeps resolving: the value a watcher took before a change reads
// the new settings after it.
func TestNamespacesUpdateAHeldState(t *testing.T) {
	n := newNamespaces([]config.Namespace{{ID: "chat"}})

	state, ok := n.Get("chat")
	if !ok {
		t.Fatal("the configured namespace is not in the map")
	}

	n.Set([]config.Namespace{{ID: "chat", PushEnabled: true}})

	if !isClosed(state.Notify) {
		t.Fatal("the held state was not notified")
	}
	if !state.Next.Value.GetPushEnabled() {
		t.Error("the held state's successor does not carry the change")
	}
}

// The map reports itself loaded straight away: this server reads its
// namespaces before it opens its listener, so nothing ever waits to find out
// whether a namespace it cannot find is absent or merely not read yet.
func TestNamespacesAreLoadedImmediately(t *testing.T) {
	select {
	case <-newNamespaces(nil).Loaded():
	default:
		t.Error("the namespace map reports itself still loading")
	}
}

// isClosed reports whether a notification channel has fired.
func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

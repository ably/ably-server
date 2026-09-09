package handles

import (
	"testing"

	"github.com/ably/ably-server/internal/config"
)

// The configured namespaces reach the map, in the terms the protocol code
// reads them in. What the map does with them is its own tests' business; what
// is this server's is that every field it can configure is carried over.
func TestSetNamespacesCarriesTheConfiguredSettings(t *testing.T) {
	app, _ := newTestApp(t, config.Namespace{
		ID:              "chat",
		Persisted:       true,
		MutableMessages: true,
		PushEnabled:     true,
		Modified:        1,
	}, config.Namespace{
		ID:       "*:edits",
		Mode:     "matcher",
		Modified: 1,
	})

	ns := app.Namespaces().MostSpecific("chat:room")
	if !ns.GetPersisted() || !ns.GetMutableMessages() || !ns.GetPushEnabled() {
		t.Errorf("chat:room resolved to %+v, want the configured settings", ns)
	}
	if got := app.Namespaces().MostSpecific("anything:edits").GetId(); got != "*:edits" {
		t.Errorf("anything:edits resolved to %q, want the matcher", got)
	}
}

// Applying namespaces is what a reload does with every namespace this server
// has, changed or not, so the version each carries is what decides whether an
// attached channel has to resolve again.
func TestSetNamespacesAppliesWhatChanged(t *testing.T) {
	app, _ := newTestApp(t, config.Namespace{ID: "chat", Modified: 1})

	// A watcher takes the notification channels the way ResolveNamespace does,
	// before reading anything.
	added, updated := app.Namespaces().Added().Notify, app.Namespaces().Updated().Notify

	// Re-reading an unedited directory costs an attached channel no work.
	app.SetNamespaces([]config.Namespace{{ID: "chat", Modified: 1}})
	if isClosed(added) || isClosed(updated) {
		t.Error("an unchanged set notified a watcher")
	}

	// A file written again is a new version of the namespace, and the index a
	// channel resolves through carries the edit.
	app.SetNamespaces([]config.Namespace{{ID: "chat", Persisted: true, Modified: 2}})
	if !isClosed(updated) {
		t.Error("an edited namespace did not notify")
	}
	if !app.Namespaces().MostSpecific("chat:room").GetPersisted() {
		t.Error("the index still resolves chat:room to the old settings")
	}

	// A namespace no longer configured leaves the map: deleting its file
	// deletes the namespace.
	app.SetNamespaces(nil)
	if _, held := app.Namespaces().Get("chat"); held {
		t.Error("a namespace that is no longer configured is still in the map")
	}
}

// A file dropped from --namespaces-dir reverts to the namespace the config
// file configured, which is an older version than the one it is replacing.
func TestSetNamespacesRevertsToAnOlderVersion(t *testing.T) {
	const startup, edited = 100, 200

	app, _ := newTestApp(t, config.Namespace{ID: "chat", Modified: startup})
	app.SetNamespaces([]config.Namespace{{ID: "chat", Persisted: true, Modified: edited}})

	app.SetNamespaces([]config.Namespace{{ID: "chat", Modified: startup}})
	if app.Namespaces().MostSpecific("chat").GetPersisted() {
		t.Error("dropping the overriding file left its settings in force")
	}
}

// The map reports itself loaded straight away: this server reads its
// namespaces before it opens its listener, so nothing ever waits to find out
// whether a namespace it cannot find is absent or merely not read yet.
func TestNamespacesAreLoadedImmediately(t *testing.T) {
	app, _ := newTestApp(t)
	select {
	case <-app.Namespaces().Loaded():
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

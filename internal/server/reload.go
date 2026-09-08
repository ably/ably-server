package server

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/handles"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/rest"
)

// This file re-reads the watched config sources while the server runs and
// applies what they say (DESIGN.md §9.1).
//
// A watched key or namespace layers over the statically configured one of the
// same id rather than replacing the set: what a flag, an environment variable
// or the config file supplied is the floor, and the directories are what moves
// on top of it. So a server started with one key on the command line still has
// that key however the keys directory is edited, and a namespace declared in
// both places is the directory's.

// reloadInterval is how often the watched sources are re-read. A directory of
// a few small files costs nothing to stat and parse once a second, and a
// second is short enough that an operator editing one does not wonder whether
// it took.
const reloadInterval = time.Second

// reloader applies the watched config sources to the running server.
type reloader struct {
	sources config.Dynamic

	// appID is the app every key must belong to, fixed at startup. This server
	// is its app (DESIGN.md §3), so a key file naming another app is a
	// misconfiguration rather than a second app appearing.
	appID string

	// staticKeys and staticNamespaces are what the flags, the environment and
	// the config file supplied. They are re-applied on every reload, since a
	// watched entry layers over them rather than replacing them.
	staticKeys       []keySpec
	staticNamespaces []config.Namespace

	app  *handles.App
	rest *rest.Server
	log  *logging.Logger

	// applied is the last snapshot that was read, so that a poll finding the
	// sources unchanged does no work. It is recorded even when applying it
	// failed, so a file that cannot be applied is complained about once rather
	// than once a second.
	applied config.Snapshot
}

// load reads the watched sources once and applies them. It is the startup
// path, where a source that cannot be read or applied is fatal: the server was
// asked to serve that configuration, and serving a different one silently is
// worse than not starting.
func (r *reloader) load() error {
	snapshot, err := r.sources.Read()
	if err != nil {
		return err
	}
	r.applied = snapshot
	return r.apply(snapshot)
}

// run re-reads the watched sources every interval until ctx is done. Nothing
// here is fatal: the server is already serving, and a half-written file is a
// reason to keep serving what was last good rather than to stop.
func (r *reloader) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// lastErr is the last complaint made, so a source that stays broken is
	// reported when it breaks rather than on every poll.
	var lastErr string
	complain := func(msg string, err error) {
		if err.Error() == lastErr {
			return
		}
		lastErr = err.Error()
		r.log.Error(msg, "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		snapshot, err := r.sources.Read()
		if err != nil {
			complain("re-reading the watched config", err)
			continue
		}
		if snapshot.Equal(r.applied) {
			continue
		}
		r.applied = snapshot
		if err := r.apply(snapshot); err != nil {
			complain("applying the watched config", err)
			continue
		}
		lastErr = ""
	}
}

// apply installs a snapshot. Everything is validated before anything is
// installed, so a snapshot this server will not serve leaves it serving what
// it already was rather than half of the new one.
func (r *reloader) apply(snapshot config.Snapshot) error {
	keys, err := r.resolveKeys(snapshot.Keys)
	if err != nil {
		return err
	}
	namespaces := mergeNamespaces(r.staticNamespaces, snapshot.Namespaces)
	if err := config.ValidateNamespaces(namespaces); err != nil {
		return err
	}

	if err := r.app.SetKeys(keys); err != nil {
		return err
	}
	r.app.SetNamespaces(namespaces)
	r.app.SetEnabled(handles.EnabledByStatus(snapshot.AppStatus))
	if r.rest != nil {
		r.rest.SetKeys(keys)
	}

	// Logged here rather than by the caller so that a server started against a
	// status file saying its app is disabled says so, as much as one disabled
	// while it runs. The status is logged as written, not as read, so a typo
	// that disabled the app is visible next to the fact that it did.
	r.log.Info("watched config applied",
		"keys", len(keys),
		"namespaces", len(namespaces),
		"appEnabled", r.app.Enabled(),
		"appStatus", cmp.Or(snapshot.AppStatus, handles.StatusEnabled),
	)
	if len(keys) == 0 {
		r.log.Warn("the app has no keys left, so nothing can authenticate against it")
	}
	return nil
}

// resolveKeys parses the static keys and the watched ones into the set the app
// should hold, the watched entry winning where both name the same key.
func (r *reloader) resolveKeys(watched []config.KeyEntry) ([]auth.APIKey, error) {
	specs := make([]keySpec, 0, len(r.staticKeys)+len(watched))
	specs = append(specs, r.staticKeys...)
	for _, entry := range watched {
		specs = append(specs, keySpec{key: entry.Key, capability: entry.Capability})
	}

	byName := make(map[string]int, len(specs))
	keys := make([]auth.APIKey, 0, len(specs))
	for _, spec := range specs {
		key, err := auth.ParseAPIKeyWithCapability(spec.key, spec.capability)
		if err != nil {
			return nil, err
		}
		if key.AppID != r.appID {
			return nil, fmt.Errorf("api key %s belongs to app %s, and this server serves %s",
				key.Name(), key.AppID, r.appID)
		}
		if at, ok := byName[key.Name()]; ok {
			keys[at] = key
			continue
		}
		byName[key.Name()] = len(keys)
		keys = append(keys, key)
	}
	return keys, nil
}

// mergeNamespaces layers the watched namespaces over the configured ones, the
// watched entry winning where both name the same id. Order is preserved so
// that what is applied does not depend on map iteration.
func mergeNamespaces(static, watched []config.Namespace) []config.Namespace {
	merged := make([]config.Namespace, 0, len(static)+len(watched))
	at := make(map[string]int, len(static)+len(watched))
	for _, ns := range slices.Concat(static, watched) {
		if i, ok := at[ns.ID]; ok {
			merged[i] = ns
			continue
		}
		at[ns.ID] = len(merged)
		merged = append(merged, ns)
	}
	return merged
}

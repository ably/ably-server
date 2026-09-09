package handles

import (
	"fmt"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"

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

// wireNamespaces is the configured namespaces in the protocol code's terms,
// ready to be given to app.NamespaceMap.Replace.
//
// Only the fields this server configures are set; the rest of an Ably
// namespace's settings have no spelling here and read as their zero values.
// Modified carries the version config loading worked out for each namespace
// (see config.Namespace.Modified), which is what the map compares to decide
// which of them actually changed.
func wireNamespaces(configured []config.Namespace) []*wire.Namespace {
	namespaces := make([]*wire.Namespace, len(configured))
	for i, c := range configured {
		namespaces[i] = &wire.Namespace{
			Id:              c.ID,
			Mode:            c.Mode,
			Persisted:       c.Persisted,
			MutableMessages: c.MutableMessages,
			PushEnabled:     c.PushEnabled,
			Modified:        uint64(c.Modified),
		}
	}
	return namespaces
}

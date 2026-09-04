package handles

import (
	"fmt"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"

	protoapp "github.com/ably/server-protocol/go/app"
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

// newNamespaces is the configured namespaces as the protocol code reads them.
// They are read once at startup, so nothing ever changes under an attached
// channel and a static map is the whole of it.
func newNamespaces(configured []config.Namespace) protoapp.NamespaceMap {
	items := make(map[string]*wire.Namespace, len(configured))
	for _, n := range configured {
		items[n.ID] = &wire.Namespace{
			Id:              n.ID,
			Mode:            n.Mode,
			Persisted:       n.Persisted,
			MutableMessages: n.MutableMessages,
			PushEnabled:     n.PushEnabled,
		}
	}
	m := protoapp.StaticItemMap(items)
	return staticNamespaces{ItemMap: m, index: protoapp.NewNamespaceIndex(m.All())}
}

// staticNamespaces is the app's namespaces ordered by specificity, which is
// what resolving a channel's namespace scans: more than one configured
// namespace can match a name, and the most specific of them is the one that
// applies.
//
// The order depends only on the namespaces themselves, and this server reads
// those once at startup, so the index built there is the only one there will
// ever be. Realtime rebuilds it whenever the set changes; here there is no
// change to rebuild for.
type staticNamespaces struct {
	protoapp.ItemMap[*wire.Namespace]
	index *protoapp.NamespaceIndex
}

var _ protoapp.NamespaceMap = staticNamespaces{}

func (n staticNamespaces) Index() *protoapp.NamespaceIndex { return n.index }

package handles

import (
	"context"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"

	"google.golang.org/protobuf/proto"

	"github.com/ably/server-protocol/go/analytics"
	protoapp "github.com/ably/server-protocol/go/app"
	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/conf"
	"github.com/ably/server-protocol/go/conf/runtime"
	"github.com/ably/server-protocol/go/errors"
	"github.com/ably/server-protocol/go/live"
	protocollog "github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/resource"
	"github.com/ably/server-protocol/go/wire"
)

// How often idle channels are looked for, and how long a channel must have
// gone unreferenced before one is collected.
//
// A channel outlives the attachments on it — two attachments and a REST
// request on the same name share one, which is the whole point of the cache it
// holds — but it must not outlive the process's interest in it, or a server a
// client has walked across ten thousand channel names is still tailing ten
// thousand channels. The interval is well above the time a client takes to
// reconnect, so a resume finds the cache it left.
const (
	gcInterval = 30 * time.Second
	gcAfter    = 2 * time.Minute
)

// ChannelManager is this server's half of the shared module's channel
// lifecycle: it makes the channel behind one name, and the module's manager
// decides when one is made, who shares it, and when it is collected.
//
// Attaching, publishing and the sync machinery are all the module's, driven
// against the channels made here — so the attach handshake, the mode
// negotiation, the capability checks and everything the client is told about
// the channel are the protocol's, and what this supplies is the channel
// itself.
type ChannelManager struct {
	manager *core.Manager

	// namespaces are the app's, as configured. A channel's settings — whether
	// its messages can be edited, whether it is persisted — come from the
	// namespace its name falls in, so a channel handed the wrong one is told
	// it cannot do things this server was configured to let it do. The map is
	// live: it changes as the app's namespaces are reconfigured under it
	// (DESIGN.md §9.1).
	namespaces protoapp.NamespaceMap

	log *logging.Logger

	// ctx bounds the work each channel does on its own behalf — tailing
	// itself into its cache, and keeping that cache within its bound. A
	// channel's own context is derived from this one, so collecting a channel
	// ends its work and stopping the server ends all of it.
	ctx  context.Context
	stop context.CancelFunc

	// public is the module's manager, which holds one channel per name and
	// hands out counted references to it. This server does not keep a map of
	// its own: two channels for one name would split the message cache and the
	// presence map between them, which is exactly what that manager exists to
	// prevent.
	public *channel.Manager
}

func NewChannelManager(manager *core.Manager, c *conf.Conf, namespaces protoapp.NamespaceMap, log *logging.Logger) *ChannelManager {
	ctx, stop := context.WithCancel(context.Background())
	m := &ChannelManager{
		manager:    manager,
		namespaces: namespaces,
		log:        log,
		ctx:        ctx,
		stop:       stop,
	}
	m.public = channel.NewManager(channel.ManagerConfig{
		NewChannel: m.newChannel,
		Conf:       c.Channel,
		// The site code is empty because this server has one site: there is no
		// other site for a serial to belong to, so no resume is another's.
		SiteCode:         "",
		AttachmentHandle: m.attachmentHandle,
		GCInterval:       runtime.NewDuration(gcInterval),
		GCAfter:          runtime.NewDuration(gcAfter),
		Log:              log.Protocol(),
	})
	return m
}

// Public is the manager the shared module holds, which is what a Protocol is
// configured with and what a channel is borrowed through.
func (m *ChannelManager) Public() *channel.Manager { return m.public }

// Start begins collecting idle channels, and Close stops both that and every
// channel's own work.
func (m *ChannelManager) Start(ctx context.Context) error { return m.public.Start(ctx) }

func (m *ChannelManager) Close() {
	// Stopping the module's manager ends the collection loop; cancelling the
	// context ends what the channels it still holds are doing, which it does
	// not stop for them.
	_ = m.public.Stop(context.Background())
	m.stop()
}

// newChannel makes the channel behind one name, which the module's manager
// calls when it holds none. It is returned unstarted, holding the cache the
// module made for it: the manager initialises that cache from this channel's
// own reads and then starts it, so nothing ever feeds or serves an empty one.
func (m *ChannelManager) newChannel(ctx context.Context, spec *channel.Spec, cache *channel.Cache) (channel.Channel, *errors.ErrorInfo) {
	id := spec.ChannelID()

	stored, err := m.manager.GetChannel(ctx, id)
	if err != nil {
		return nil, errors.New(50000, 500, "channel unavailable: %s", err)
	}

	log := m.log.Protocol().With("channel", id)
	c := newLiveChannel(m.ctx, stored, cache, m.namespaceFor(spec), log)

	// A leave held for a connection that never comes back is published through
	// the channel it was held for, which is why this is set here rather than
	// passed in: the channel has to exist before there is anything to publish
	// through.
	c.leaves = newDelayedLeaves(m.ctx, func(ctx context.Context, leave *wire.ChannelMessage) {
		if err := c.Publish(ctx, leave, nil); err != nil {
			log.Warn("unable to publish a held presence leave", "err", err)
		}
	}, log)

	return c, nil
}

// namespaceFor is the namespace whose settings a channel takes: the most
// specific configured namespace matching its name, combined with whatever the
// channel's directives imply. A name no namespace matches resolves to an empty
// namespace, which is a channel with default settings rather than an error.
//
// The mapping is the module's rather than this server's, because a namespace
// id is no longer just a channel's first segment: a rule can be written as a
// match expression, several can match one name, and which of them wins is
// decided by specificity. A server resolving that itself would tell a channel
// something different about itself than realtime does.
func (m *ChannelManager) namespaceFor(spec *channel.Spec) *namespaceWatch {
	resolved := channel.ResolveNamespace(spec, m.namespaces)
	return &namespaceWatch{
		spec:       spec,
		namespaces: m.namespaces,
		resolved:   resolved,
		value:      live.NewValue(resolved.Namespace),
	}
}

// namespaceWatch is one channel's namespace, kept resolved. This server's
// namespaces can be reconfigured while it runs (DESIGN.md §9.1), and which
// namespace applies to a channel is decided by matching every configured one
// against its name — so a namespace added, changed or removed anywhere can
// change what an already attached channel is allowed to do.
type namespaceWatch struct {
	spec       *channel.Spec
	namespaces protoapp.NamespaceMap

	// resolved is the last resolution, held for the notifications it carries:
	// waiting on those is how the loop below learns to resolve again.
	resolved channel.ResolvedNamespace

	// value is what the channel hands to whoever asks for its namespace, and
	// what they watch.
	value *live.Value[*wire.Namespace]
}

// run re-resolves the channel's namespace whenever the app's namespaces
// change, until ctx is done. The value is only set when the resolution
// actually differs: a namespace added elsewhere in the app notifies every
// channel, and all but the few it applies to should see nothing.
func (w *namespaceWatch) run(ctx context.Context) {
	for w.resolved.WaitForChange(ctx) {
		w.resolved = channel.ResolveNamespace(w.spec, w.namespaces)
		if !proto.Equal(w.resolved.Namespace, w.value.Get()) {
			w.value.Set(w.resolved.Namespace)
		}
	}
}

// attachmentHandle is what this server hands one attachment: somewhere to log,
// and nothing else. Realtime's also carries the analytics service the
// attachment's intervals are collected by and the app's publish rate limits;
// this server collects neither and limits nothing.
func (m *ChannelManager) attachmentHandle(_ context.Context, req channel.AttachRequest, _ channel.Connection) channel.AttachmentHandle {
	return attachmentHandle{log: m.log.Protocol().With("channel", req.Channel)}
}

type attachmentHandle struct {
	log protocollog.Logger
}

var _ channel.AttachmentHandle = attachmentHandle{}

func (h attachmentHandle) Log() protocollog.Logger { return h.log }

func (h attachmentHandle) AnalyticsCollector(time.Time, string, resource.Qualifier, *live.Value[*wire.Namespace]) analytics.AttachmentCollector {
	return analytics.NopCollector{}
}

// PublishRateCounter admits publishes at up to the rate a token's limit claim
// asks for. This server enforces no such limit, so nothing is delayed.
func (h attachmentHandle) PublishRateCounter(int64) channel.DelayRateCounter {
	return channel.NopDelayRateCounter
}

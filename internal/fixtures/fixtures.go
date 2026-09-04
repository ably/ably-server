// Package fixtures pre-seeds channels with presence members declared in
// the config file's [[channels]] section (DESIGN.md §9, §12.5). It exists
// purely for SDK test-suite compatibility: the cloud sandbox provisions
// these members when it creates a test app, and Ably SDK presence
// tests read them back, so the local server needs an equivalent seed to
// run those suites. The provisioner translates the test-app-setup JSON into
// config; this package only consumes the resulting Spec.
package fixtures

import (
	"context"
	"fmt"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/id"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/server-protocol/go/wire"
)

// Spec is the set of channels to seed.
type Spec struct {
	Channels []Channel
}

// Channel is one channel's presence fixtures.
type Channel struct {
	Name     string
	Presence []Member
}

// Member is one presence member to seed. Data and Encoding round-trip
// verbatim — the server treats Encoding as opaque and never decodes
// Data (DESIGN.md §9), so a cipher payload is stored exactly as given.
type Member struct {
	ClientID string
	Data     any
	Encoding string
}

// Seed enters every spec member into its channel through the normal
// StorePresence path (DESIGN.md §9, §12), so each member lands in both
// the membership set and presence history. Members are entered as static
// fixtures (storage.WithStaticPresence): they belong to no connection —
// so no teardown LEAVE — and carry a non-expiring lease in cluster mode
// so the reaper never removes them. Each member is given a fresh
// server-synthesized connectionId; clientId/data/encoding are preserved
// verbatim.
func Seed(ctx context.Context, m *core.Manager, spec *Spec, logger *logging.Logger) error {
	seedCtx := storage.WithStaticPresence(ctx)
	now := time.Now().UnixMilli()
	for _, ch := range spec.Channels {
		if len(ch.Presence) == 0 {
			continue
		}
		channel, err := m.GetChannel(ctx, ch.Name)
		if err != nil {
			return fmt.Errorf("fixtures: get channel %q: %w", ch.Name, err)
		}
		members := make([]*wire.PresenceMessage, 0, len(ch.Presence))
		for _, mem := range ch.Presence {
			data, encoding, err := wire.DataFromExternal(mem.Data, mem.Encoding)
			if err != nil {
				return fmt.Errorf("fixtures: channel %q member %q data: %w", ch.Name, mem.ClientID, err)
			}
			members = append(members, &wire.PresenceMessage{
				Action:       wire.PresenceMessage_ENTER,
				ClientId:     &mem.ClientID,
				ConnectionId: id.NewConnectionID(),
				Data:         data,
				Encoding:     encoding,
				Timestamp:    uint64(now),
			})
		}
		if _, _, err := channel.PublishPresence(seedCtx, members); err != nil {
			return fmt.Errorf("fixtures: seed channel %q: %w", ch.Name, err)
		}
		if logger != nil {
			logger.Info("seeded presence fixtures", "channel", ch.Name, "members", len(members))
		}
	}
	return nil
}

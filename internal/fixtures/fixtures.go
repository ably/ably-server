// Package fixtures pre-seeds channels with presence members from an
// Ably test-app-setup-shaped JSON file (--fixtures, DESIGN.md §9). It
// exists purely for SDK test-suite compatibility: the cloud sandbox
// provisions these members when it creates a test app, and the ably-go
// presence tests read them back, so the local server needs an
// equivalent seed to run that suite.
package fixtures

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/id"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// Spec is the parsed set of channels to seed. It is decoded from a
// test-app-setup-shaped JSON file — either the full {post_apps:{channels:[…]}}
// shape or a bare {channels:[…]} — keeping only the parts the seed needs.
type Spec struct {
	Channels []Channel
}

// Channel is one channel's presence fixtures.
type Channel struct {
	Name     string   `json:"name"`
	Presence []Member `json:"presence"`
}

// Member is one presence member to seed. Data and Encoding round-trip
// verbatim — the server treats Encoding as opaque and never decodes
// Data (DESIGN.md §12), so a cipher payload is stored exactly as given.
type Member struct {
	ClientID string `json:"clientId"`
	Data     any    `json:"data"`
	Encoding string `json:"encoding,omitempty"`
}

// Load reads and parses the fixtures spec at path. A missing/unreadable
// path or a malformed spec is an error, surfaced by the caller as a
// startup failure (DESIGN.md §9).
func Load(path string) (*Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("fixtures: read %q: %w", path, err)
	}
	spec, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("fixtures: %q: %w", path, err)
	}
	return spec, nil
}

// Parse decodes a fixtures spec from JSON, accepting both the full
// {post_apps:{channels:[…]}} shape and a bare {channels:[…]}. Unknown
// keys (limits, keys, namespaces, cipher, …) are ignored. A spec that
// parses but names no channels, an unnamed channel, or a member with no
// clientId is rejected as malformed.
func Parse(data []byte) (*Spec, error) {
	var raw struct {
		PostApps *struct {
			Channels []Channel `json:"channels"`
		} `json:"post_apps"`
		Channels []Channel `json:"channels"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	channels := raw.Channels
	if raw.PostApps != nil && len(raw.PostApps.Channels) > 0 {
		channels = raw.PostApps.Channels
	}
	if len(channels) == 0 {
		return nil, errors.New("spec contains no channels")
	}
	for ci, ch := range channels {
		if ch.Name == "" {
			return nil, fmt.Errorf("channel #%d has no name", ci)
		}
		for mi, m := range ch.Presence {
			if m.ClientID == "" {
				return nil, fmt.Errorf("channel %q presence member #%d has no clientId", ch.Name, mi)
			}
		}
	}
	return &Spec{Channels: channels}, nil
}

// Seed enters every spec member into its channel through the normal
// StorePresence path (DESIGN.md §9, §12), so each member lands in both
// the membership set and presence history. Members are entered as static
// fixtures (storage.WithStaticPresence): they belong to no connection —
// so no teardown LEAVE — and carry a non-expiring lease in cluster mode
// so the reaper never removes them. Each member is given a fresh
// server-synthesized connectionId; clientId/data/encoding are preserved
// verbatim.
func Seed(ctx context.Context, m *core.Manager, spec *Spec, logger *slog.Logger) error {
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
		members := make([]*protocol.PresenceMessage, 0, len(ch.Presence))
		for _, mem := range ch.Presence {
			members = append(members, &protocol.PresenceMessage{
				Action:       protocol.PresenceEnter,
				ClientID:     mem.ClientID,
				ConnectionID: id.NewConnectionID(),
				Data:         mem.Data,
				Encoding:     mem.Encoding,
				Timestamp:    now,
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

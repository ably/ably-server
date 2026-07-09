package fixtures_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/fixtures"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

func TestParse_PostAppsShape(t *testing.T) {
	spec, err := fixtures.Parse([]byte(`{
		"limits": {"presence": {"maxMembers": 250}},
		"post_apps": {"channels": [
			{"name": "c1", "presence": [{"clientId": "a", "data": "true"}]}
		]}
	}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spec.Channels) != 1 || spec.Channels[0].Name != "c1" {
		t.Fatalf("unexpected channels: %+v", spec.Channels)
	}
	if got := spec.Channels[0].Presence[0].ClientID; got != "a" {
		t.Fatalf("clientId = %q, want a", got)
	}
}

func TestParse_BareChannelsShape(t *testing.T) {
	spec, err := fixtures.Parse([]byte(`{"channels": [
		{"name": "c1", "presence": [{"clientId": "a", "data": "1"}]}
	]}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spec.Channels) != 1 {
		t.Fatalf("want 1 channel, got %d", len(spec.Channels))
	}
}

func TestParse_Malformed(t *testing.T) {
	cases := map[string]string{
		"invalid json":     `{not json`,
		"no channels":      `{"post_apps": {"keys": []}}`,
		"empty channels":   `{"channels": []}`,
		"unnamed channel":  `{"channels": [{"presence": []}]}`,
		"member no client": `{"channels": [{"name": "c1", "presence": [{"data": "x"}]}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := fixtures.Parse([]byte(body)); err == nil {
				t.Fatalf("Parse(%q) = nil error, want error", body)
			}
		})
	}
}

func TestLoad_BadPath(t *testing.T) {
	if _, err := fixtures.Load(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Fatal("Load of missing path = nil error, want error")
	}
}

// TestSeed_MembersHistoryAndEncodings seeds the testdata spec into a
// memory-backed manager and asserts the members land in both the
// membership set and presence history with data/encoding verbatim
// (AC #1, #2).
func TestSeed_MembersHistoryAndEncodings(t *testing.T) {
	spec, err := fixtures.Load(filepath.Join("testdata", "presence-fixtures.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	mgr := core.NewManager(memory.New(memory.Options{}))
	ctx := context.Background()
	if err := fixtures.Seed(ctx, mgr, spec, nil); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	ch, err := mgr.GetChannel(ctx, "persisted:presence_fixtures")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}

	// Expected verbatim data/encoding per member (encoding opaque, data
	// stored exactly as the spec gave it).
	wantData := map[string]any{
		"client_bool":    "true",
		"client_int":     "24",
		"client_string":  "This is a string clientData payload",
		"client_json":    `{ "test": "This is a JSONObject clientData payload"}`,
		"client_decoded": `{"example":{"json":"Object"}}`,
		"client_encoded": "HO4cYSP8LybPYBPZPHQOtuD53yrD3YV3NBoTEYBh4U0N1QXHbtkfsDfTspKeLQFt",
	}
	wantEncoding := map[string]string{
		"client_decoded": "json",
		"client_encoded": "json/utf-8/cipher+aes-128-cbc/base64",
	}

	// Membership set.
	members, _, err := ch.Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(members) != len(wantData) {
		t.Fatalf("Members len = %d, want %d", len(members), len(wantData))
	}
	seenConn := map[string]bool{}
	for _, m := range members {
		wd, ok := wantData[m.ClientID]
		if !ok {
			t.Errorf("unexpected member clientId %q", m.ClientID)
			continue
		}
		if m.Data != wd {
			t.Errorf("%s: data = %#v, want %#v", m.ClientID, m.Data, wd)
		}
		if m.Encoding != wantEncoding[m.ClientID] {
			t.Errorf("%s: encoding = %q, want %q", m.ClientID, m.Encoding, wantEncoding[m.ClientID])
		}
		if m.ConnectionID == "" {
			t.Errorf("%s: empty connectionId, want a synthesized one", m.ClientID)
		}
		if seenConn[m.ConnectionID] {
			t.Errorf("%s: connectionId %q reused across members", m.ClientID, m.ConnectionID)
		}
		seenConn[m.ConnectionID] = true
	}

	// Presence history (forwards) — every member appears as a stored ENTER.
	page, err := ch.History(ctx, storage.HistoryQuery{
		Kind:      storage.KindPresence,
		Direction: storage.DirectionForwards,
	})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var hist []*protocol.PresenceMessage
	for _, cm := range page.ChannelMessages {
		hist = append(hist, cm.Presence...)
	}
	if len(hist) != len(wantData) {
		t.Fatalf("presence history len = %d, want %d", len(hist), len(wantData))
	}
	for _, p := range hist {
		if p.Action != protocol.PresenceEnter {
			t.Errorf("%s: history action = %v, want ENTER", p.ClientID, p.Action)
		}
		if wd := wantData[p.ClientID]; p.Data != wd {
			t.Errorf("%s: history data = %#v, want %#v", p.ClientID, p.Data, wd)
		}
	}
}

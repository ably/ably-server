package fixtures_test

import (
	"context"
	"testing"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/fixtures"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

// presenceFixturesSpec mirrors the persisted:presence_fixtures channel the
// cloud sandbox provisions (ably-common test-app-setup.json), the members
// Ably SDK presence test suites read back.
func presenceFixturesSpec() *fixtures.Spec {
	return &fixtures.Spec{Channels: []fixtures.Channel{{
		Name: "persisted:presence_fixtures",
		Presence: []fixtures.Member{
			{ClientID: "client_bool", Data: "true"},
			{ClientID: "client_int", Data: "24"},
			{ClientID: "client_string", Data: "This is a string clientData payload"},
			{ClientID: "client_json", Data: `{ "test": "This is a JSONObject clientData payload"}`},
			{ClientID: "client_decoded", Data: `{"example":{"json":"Object"}}`, Encoding: "json"},
			{ClientID: "client_encoded", Data: "HO4cYSP8LybPYBPZPHQOtuD53yrD3YV3NBoTEYBh4U0N1QXHbtkfsDfTspKeLQFt", Encoding: "json/utf-8/cipher+aes-128-cbc/base64"},
		},
	}}}
}

// TestSeed_MembersHistoryAndEncodings seeds the spec into a memory-backed
// manager and asserts the members land in both the membership set and
// presence history with data/encoding verbatim (AC #1).
func TestSeed_MembersHistoryAndEncodings(t *testing.T) {
	spec := presenceFixturesSpec()

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
		// Fixture members are genuinely synthesized: no real connection or
		// msgSerial, so they stay id-less and SDKs order them by timestamp
		// (RTP2b1), never taking the id path.
		if m.ID != "" {
			t.Errorf("%s: fixture member carries id %q, want id-less", m.ClientID, m.ID)
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

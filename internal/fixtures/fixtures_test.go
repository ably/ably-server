package fixtures_test

import (
	"context"
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/fixtures"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
	"github.com/ably/server-protocol/go/wire"
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

	// Expected data/encoding per member. A payload is stored as the spec gave
	// it, except that an outer base64 is decoded on the way in: base64 is how
	// a JSON transport carries bytes, not part of what the payload is, so it
	// comes off with the rest of the transport and the suffix comes off the
	// encoding with it (matching the reference).
	wantData := map[string]any{
		"client_bool":    "true",
		"client_int":     "24",
		"client_string":  "This is a string clientData payload",
		"client_json":    `{ "test": "This is a JSONObject clientData payload"}`,
		"client_decoded": `{"example":{"json":"Object"}}`,
		"client_encoded": mustBase64(t, "HO4cYSP8LybPYBPZPHQOtuD53yrD3YV3NBoTEYBh4U0N1QXHbtkfsDfTspKeLQFt"),
	}
	wantEncoding := map[string]string{
		"client_decoded": "json",
		"client_encoded": "json/utf-8/cipher+aes-128-cbc",
	}

	// Membership set.
	chPage, err := ch.Members(ctx, storage.MembersQuery{})
	members, _ := chPage.Members, chPage.AsOfSerial
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(members) != len(wantData) {
		t.Fatalf("Members len = %d, want %d", len(members), len(wantData))
	}
	seenConn := map[string]bool{}
	for _, m := range members {
		wd, ok := wantData[m.GetClientId()]
		if !ok {
			t.Errorf("unexpected member clientId %q", m.GetClientId())
			continue
		}
		if !reflect.DeepEqual(m.Data.AsAny(), wd) {
			t.Errorf("%s: data = %#v, want %#v", m.GetClientId(), m.Data.AsAny(), wd)
		}
		if m.Encoding != wantEncoding[m.GetClientId()] {
			t.Errorf("%s: encoding = %q, want %q", m.GetClientId(), m.Encoding, wantEncoding[m.GetClientId()])
		}
		if m.ConnectionId == "" {
			t.Errorf("%s: empty connectionId, want a synthesized one", m.GetClientId())
		}
		// Fixture members are genuinely synthesized: no real connection or
		// msgSerial, so they stay id-less and SDKs order them by timestamp
		// (RTP2b1), never taking the id path.
		if m.GetId() != "" {
			t.Errorf("%s: fixture member carries id %q, want id-less", m.GetClientId(), m.GetId())
		}
		if seenConn[m.ConnectionId] {
			t.Errorf("%s: connectionId %q reused across members", m.GetClientId(), m.ConnectionId)
		}
		seenConn[m.ConnectionId] = true
	}

	// Presence history (forwards) — every member appears as a stored ENTER.
	page, err := ch.History(ctx, storage.HistoryQuery{
		Kind:      storage.KindPresence,
		Direction: storage.DirectionForwards,
	})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var hist []*wire.PresenceMessage
	for _, cm := range page.ChannelMessages {
		hist = append(hist, cm.Presence...)
	}
	if len(hist) != len(wantData) {
		t.Fatalf("presence history len = %d, want %d", len(hist), len(wantData))
	}
	for _, p := range hist {
		if p.Action != wire.PresenceMessage_ENTER {
			t.Errorf("%s: history action = %v, want ENTER", p.GetClientId(), p.Action)
		}
		if wd := wantData[p.GetClientId()]; !reflect.DeepEqual(p.Data.AsAny(), wd) {
			t.Errorf("%s: history data = %#v, want %#v", p.GetClientId(), p.Data.AsAny(), wd)
		}
	}
}

// mustBase64 decodes a base64 payload, which is how the fixture spec carries
// bytes and how they come back off it.
func mustBase64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	return b
}

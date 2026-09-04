package handles

import "testing"

// Protocol actions this test speaks, as the wire numbers the frames carry.
const (
	actionACK      = 1
	actionAttached = 11
	actionSync     = 16
	actionPresence = 14
)

// hasPresenceFlag is the ATTACHED flag telling a client a presence sync is
// coming, so that it waits for one rather than concluding the channel is empty.
const hasPresenceFlag = 1 << 0

// TestServeWebSocket_PresenceEnterIsSyncedToAnotherConnection is the flow
// ably-js's presenceGetUnattached exercises: one connection enters presence,
// another attaches and is told about it.
//
// It is a regression test for reporting no presence rather than no knowledge.
// This server keeps no occupancy, and the protocol code used to read that as
// the channel having no members — so the ATTACHED carried no HAS_PRESENCE, no
// sync followed, and a client asking who was present was told nobody, forever.
func TestServeWebSocket_PresenceEnterIsSyncedToAnotherConnection(t *testing.T) {
	stack := newTestServers(t)

	enterer := dial(t, stack)
	defer enterer.Close()
	readFrame(t, enterer) // CONNECTED

	if err := enterer.WriteJSON(map[string]any{"action": 10, "channel": "presence-sync"}); err != nil {
		t.Fatalf("ATTACH: %s", err)
	}
	if attached := readFrame(t, enterer); actionOf(attached) != actionAttached {
		t.Fatalf("did not attach: %v", attached)
	}

	if err := enterer.WriteJSON(map[string]any{
		"action":    actionPresence,
		"channel":   "presence-sync",
		"msgSerial": 0,
		"presence":  []map[string]any{{"action": 2, "clientId": "alice", "data": "hi"}},
	}); err != nil {
		t.Fatalf("ENTER: %s", err)
	}
	// The enterer is attached, so it sees its own sync and its own enter before
	// the ACK; read on until the publish is acknowledged.
	for actionOf(readFrame(t, enterer)) != actionACK {
	}

	// A second connection attaches, and must be told there is presence to wait
	// for before it is sent it.
	syncer := dial(t, stack)
	defer syncer.Close()
	readFrame(t, syncer) // CONNECTED
	if err := syncer.WriteJSON(map[string]any{"action": 10, "channel": "presence-sync"}); err != nil {
		t.Fatalf("syncer ATTACH: %s", err)
	}

	attached := readFrame(t, syncer)
	if actionOf(attached) != actionAttached {
		t.Fatalf("syncer did not attach: %v", attached)
	}
	flags, _ := attached["flags"].(float64)
	if int(flags)&hasPresenceFlag == 0 {
		t.Fatalf("ATTACHED did not set HAS_PRESENCE, so a client would report the channel empty: flags=%v", attached["flags"])
	}

	sync := readFrame(t, syncer)
	if actionOf(sync) != actionSync {
		t.Fatalf("got action %v, want SYNC (%d): %v", sync["action"], actionSync, sync)
	}
	members, _ := sync["presence"].([]any)
	if len(members) != 1 {
		t.Fatalf("SYNC carried %d members, want 1: %v", len(members), sync)
	}
	member, _ := members[0].(map[string]any)
	if clientID, _ := member["clientId"].(string); clientID != "alice" {
		t.Errorf("synced member clientId = %q, want alice", clientID)
	}
	// The set is reported as PRESENT (1) whatever action put a member in it.
	if action, _ := member["action"].(float64); int(action) != 1 {
		t.Errorf("synced member action = %v, want PRESENT (1)", member["action"])
	}
}

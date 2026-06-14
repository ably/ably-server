package rest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
)

// enterPresence folds presence ops into a channel via the shared Manager
// (the REST endpoints are read-only, so tests seed presence directly).
func enterPresence(t *testing.T, manager *core.Manager, channel string, pms ...*protocol.PresenceMessage) {
	t.Helper()
	ctx := context.Background()
	ch, err := manager.GetChannel(ctx, channel)
	if err != nil {
		t.Fatalf("GetChannel %q: %v", channel, err)
	}
	if _, _, err := ch.PublishPresence(ctx, pms); err != nil {
		t.Fatalf("PublishPresence %q: %v", channel, err)
	}
}

// getAuthed issues an authenticated GET with an optional Accept header.
func getAuthed(t *testing.T, srv *httptest.Server, path, accept string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("app.key", "secret")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func decodePresenceJSON(t *testing.T, resp *http.Response) []*protocol.PresenceMessage {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out []*protocol.PresenceMessage
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode body %q: %v", body, err)
	}
	return out
}

func presenceByClient(members []*protocol.PresenceMessage) map[string]*protocol.PresenceMessage {
	out := make(map[string]*protocol.PresenceMessage, len(members))
	for _, m := range members {
		out[m.ClientID] = m
	}
	return out
}

func TestPresenceCurrentSet(t *testing.T) {
	srv, manager := newTestServer(t)
	enterPresence(t, manager, "room",
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "c1", Data: "hi"})
	enterPresence(t, manager, "room",
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "bob", ConnectionID: "c2"})

	resp := getAuthed(t, srv, "/channels/room/presence", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	members := decodePresenceJSON(t, resp)
	if len(members) != 2 {
		t.Fatalf("members = %d, want 2", len(members))
	}
	byID := presenceByClient(members)
	if byID["alice"] == nil || byID["bob"] == nil {
		t.Fatalf("members = %+v, want alice and bob", members)
	}
	for _, m := range members {
		if m.Action != protocol.PresencePresent {
			t.Errorf("member %q action = %v, want present", m.ClientID, m.Action)
		}
	}
}

func TestPresenceEmptySetReturnsEmptyArray(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := getAuthed(t, srv, "/channels/room/presence", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "[]" {
		t.Errorf("body = %q, want []", body)
	}
}

func TestPresenceCurrentSetMsgpack(t *testing.T) {
	srv, manager := newTestServer(t)
	enterPresence(t, manager, "room",
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "c1"})

	resp := getAuthed(t, srv, "/channels/room/presence", "application/x-msgpack")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-msgpack" {
		t.Errorf("Content-Type = %q, want application/x-msgpack", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	var members []*protocol.PresenceMessage
	if err := msgpack.Unmarshal(body, &members); err != nil {
		t.Fatalf("msgpack decode: %v", err)
	}
	if len(members) != 1 || members[0].ClientID != "alice" {
		t.Fatalf("members = %+v, want [alice]", members)
	}
}

func TestPresenceHistory(t *testing.T) {
	srv, manager := newTestServer(t)
	// One member's lifecycle: enter, update, leave — all retained in the
	// presence stream even though the resulting set is empty.
	enterPresence(t, manager, "room", &protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "c1"})
	enterPresence(t, manager, "room", &protocol.PresenceMessage{Action: protocol.PresenceUpdate, ClientID: "alice", ConnectionID: "c1"})
	enterPresence(t, manager, "room", &protocol.PresenceMessage{Action: protocol.PresenceLeave, ClientID: "alice", ConnectionID: "c1"})

	// Default direction is backwards (newest first).
	resp := getAuthed(t, srv, "/channels/room/presence/history", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	hist := decodePresenceJSON(t, resp)
	if len(hist) != 3 {
		t.Fatalf("history len = %d, want 3", len(hist))
	}
	wantActions := []protocol.PresenceAction{protocol.PresenceLeave, protocol.PresenceUpdate, protocol.PresenceEnter}
	for i, want := range wantActions {
		if hist[i].Action != want {
			t.Errorf("history[%d] action = %v, want %v", i, hist[i].Action, want)
		}
	}
}

// TestPresenceHistoryExcludesMessages: the presence-history endpoint
// returns only presence cms, and message history excludes presence.
func TestPresenceHistoryExcludesMessages(t *testing.T) {
	srv, manager := newTestServer(t)
	publishBatch(t, srv, "room", []*protocol.Message{{Name: "m"}})
	enterPresence(t, manager, "room", &protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "c1"})

	presHist := decodePresenceJSON(t, getAuthed(t, srv, "/channels/room/presence/history?direction=forwards", ""))
	if len(presHist) != 1 || presHist[0].ClientID != "alice" {
		t.Fatalf("presence history = %+v, want exactly alice", presHist)
	}

	msgHist := decodeHistoryJSON(t, historyGet(t, srv, "room", "direction=forwards", ""))
	if len(msgHist) != 1 || msgHist[0].Name != "m" {
		t.Fatalf("message history = %+v, want exactly the message", msgHist)
	}
}

func TestPresenceHistoryPagination(t *testing.T) {
	srv, manager := newTestServer(t)
	for _, c := range []string{"a", "b", "c", "d"} {
		enterPresence(t, manager, "room", &protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: c, ConnectionID: c})
	}

	// Page 1: forwards, limit 2.
	resp := getAuthed(t, srv, "/channels/room/presence/history?direction=forwards&limit=2", "")
	page1 := decodePresenceJSON(t, resp)
	if len(page1) != 2 || page1[0].ClientID != "a" || page1[1].ClientID != "b" {
		t.Fatalf("page1 = %+v, want [a b]", page1)
	}
	nextURL := nextLink(t, resp.Header.Get("Link"))
	if nextURL == "" {
		t.Fatal("page1 missing rel=next link")
	}

	// Page 2: follow the opaque next link verbatim.
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+nextURL, nil)
	req.SetBasicAuth("app.key", "secret")
	resp2, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("page2 do: %v", err)
	}
	t.Cleanup(func() { resp2.Body.Close() })
	page2 := decodePresenceJSON(t, resp2)
	if len(page2) != 2 || page2[0].ClientID != "c" || page2[1].ClientID != "d" {
		t.Fatalf("page2 = %+v, want [c d]", page2)
	}
}

// TestPresenceNoWriteRoute: presence is realtime-only — there is no REST
// write surface, so a POST to the presence path is not allowed.
func TestPresenceNoWriteRoute(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := request(t, srv, http.MethodPost, "/channels/room/presence", "application/json", []byte("{}"), true)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /channels/room/presence status = %d, want 405", resp.StatusCode)
	}
}

func TestPresenceRequiresAuth(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, path := range []string{"/channels/room/presence", "/channels/room/presence/history"} {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("do %s: %v", path, err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s status = %d, want 401", path, resp.StatusCode)
		}
	}
}

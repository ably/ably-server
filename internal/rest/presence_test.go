package rest

import (
	"context"
	"encoding/json"
	"fmt"
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

// TestPresenceHistoryIgnoresFromSerial locks in that the REST
// fromSerial/untilAttached bound has no effect on presence
// history: ably-js's RealtimePresence.history({untilAttach: true})
// never targets this endpoint, and the reference server explicitly
// doesn't support fromSerial for presence either (there's no live
// presence cache to anchor against). Bounding presence by an arbitrary
// channelSerial would be surprising and unspecified, so
// HandlePresenceHistory clears whatever parseHistoryQuery's shared
// parsing set (internal/rest/server.go).
func TestPresenceHistoryIgnoresFromSerial(t *testing.T) {
	srv, manager := newTestServer(t)
	ctx := context.Background()
	ch, err := manager.GetChannel(ctx, "room")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	cm, _, err := ch.PublishPresence(ctx, []*protocol.PresenceMessage{
		{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "c1"},
	})
	if err != nil {
		t.Fatalf("PublishPresence: %v", err)
	}
	boundSerial := cm.ChannelSerial

	enterPresence(t, manager, "room",
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "bob", ConnectionID: "c2"})

	resp := getAuthed(t, srv, "/channels/room/presence/history?fromSerial="+boundSerial, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	hist := decodePresenceJSON(t, resp)
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2 (fromSerial must not bound presence history)", len(hist))
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
	nextURL := nextLink(t, resp)
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

// TestPresenceHistoryDirectionPagination pins direction-qualified
// presence-history reads (RSP4b2): with five members entered in order
// a..e, a forwards scan returns them oldest-first and a backwards scan
// newest-first, and both walk the full set across limit=2 pages via the
// opaque rel=next cursor without dropping or repeating a member.
func TestPresenceHistoryDirectionPagination(t *testing.T) {
	for _, tc := range []struct {
		direction string
		want      []string
	}{
		{"forwards", []string{"a", "b", "c", "d", "e"}},
		{"backwards", []string{"e", "d", "c", "b", "a"}},
	} {
		t.Run(tc.direction, func(t *testing.T) {
			srv, manager := newTestServer(t)
			for _, c := range []string{"a", "b", "c", "d", "e"} {
				enterPresence(t, manager, "room", &protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: c, ConnectionID: c})
			}

			var got []string
			pages := 0
			nextURL := "/channels/room/presence/history?direction=" + tc.direction + "&limit=2"
			for nextURL != "" {
				req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+nextURL, nil)
				if err != nil {
					t.Fatalf("new request: %v", err)
				}
				req.SetBasicAuth("app.key", "secret")
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Fatalf("page %d do: %v", pages+1, err)
				}
				page := decodePresenceJSON(t, resp)
				for _, m := range page {
					got = append(got, m.ClientID)
				}
				pages++
				nextURL = nextLink(t, resp)
				resp.Body.Close()
				if pages > 5 {
					t.Fatal("pagination did not terminate")
				}
			}

			if !equalStrings(got, tc.want) {
				t.Fatalf("direction=%s paged order = %v, want %v", tc.direction, got, tc.want)
			}
			// 5 members at limit 2 => 3 pages (2, 2, 1).
			if pages != 3 {
				t.Fatalf("direction=%s paged %d times, want 3", tc.direction, pages)
			}
		})
	}
}

// TestPresenceGetPaginatesByLimit seeds six members and pages the
// presence set with limit=2: three full pages of two, a rel=next link on
// all but the last, and the full set recovered across pages (RSP3a1).
func TestPresenceGetPaginatesByLimit(t *testing.T) {
	srv, manager := newTestServer(t)
	for i := range 6 {
		enterPresence(t, manager, "room", &protocol.PresenceMessage{
			Action:       protocol.PresenceEnter,
			ClientID:     fmt.Sprintf("c%d", i),
			ConnectionID: fmt.Sprintf("conn%d", i),
			Data:         fmt.Sprintf("d%d", i),
		})
	}

	seen := map[string]struct{}{}
	nextURL := "/channels/room/presence?limit=2"
	pages := 0
	for nextURL != "" {
		resp := getAuthed(t, srv, nextURL, "")
		got := decodePresenceJSON(t, resp)
		pages++
		if len(got) != 2 {
			t.Fatalf("page %d got %d items, want 2", pages, len(got))
		}
		for _, m := range got {
			if m.Action != protocol.PresencePresent {
				t.Errorf("page %d member %s action = %v, want PRESENT", pages, m.ClientID, m.Action)
			}
			seen[m.ClientID] = struct{}{}
		}
		nextURL = nextLink(t, resp)
	}
	if pages != 3 {
		t.Fatalf("paged %d times, want 3", pages)
	}
	if len(seen) != 6 {
		t.Fatalf("saw %d distinct members across pages, want 6", len(seen))
	}
}

// TestPresenceGetFiltersByClientID asserts the clientId query param
// filters the returned set to the matching member (RSP3a2).
func TestPresenceGetFiltersByClientID(t *testing.T) {
	srv, manager := newTestServer(t)
	enterPresence(t, manager, "room",
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "connA"},
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "bob", ConnectionID: "connB"},
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "carol", ConnectionID: "connC"},
	)

	resp := getAuthed(t, srv, "/channels/room/presence?clientId=bob", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got := decodePresenceJSON(t, resp)
	if len(got) != 1 || got[0].ClientID != "bob" {
		t.Fatalf("clientId=bob returned %+v, want single member bob", got)
	}
	if nextLink(t, resp) != "" {
		t.Error("single-member filtered result should have no next link")
	}
}

// TestPresenceGetFiltersByConnectionID asserts the connectionId query
// param filters the returned set to members on that connection (RSP3a3).
func TestPresenceGetFiltersByConnectionID(t *testing.T) {
	srv, manager := newTestServer(t)
	enterPresence(t, manager, "room",
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "alice", ConnectionID: "connA"},
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "bob", ConnectionID: "connB"},
		&protocol.PresenceMessage{Action: protocol.PresenceEnter, ClientID: "carol", ConnectionID: "connC"},
	)

	resp := getAuthed(t, srv, "/channels/room/presence?connectionId=connB", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got := decodePresenceJSON(t, resp)
	if len(got) != 1 || got[0].ClientID != "bob" || got[0].ConnectionID != "connB" {
		t.Fatalf("connectionId=connB returned %+v, want single member bob/connB", got)
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

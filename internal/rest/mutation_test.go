package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
)

// versionsGet issues GET /channels/{channel}/messages/{serial}/versions.
func versionsGet(t *testing.T, srv *httptest.Server, channel, serial, rawQuery string) *http.Response {
	t.Helper()
	u := srv.URL + "/channels/" + channel + "/messages/" + serial + "/versions"
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("app.key", "secret")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// storageVersionSerial returns a message's per-version serial.
func storageVersionSerial(m *protocol.Message) string {
	if m.Version != nil && m.Version.Serial != "" {
		return m.Version.Serial
	}
	return m.Serial
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// publishOne publishes a single create and returns its stamped serial.
func publishOne(t *testing.T, srv *httptest.Server, channel string, m *protocol.Message) string {
	t.Helper()
	body, _ := json.Marshal(m)
	resp := request(t, srv, http.MethodPost, "/channels/"+channel+"/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish: status %d, want 201", resp.StatusCode)
	}
	// Read the create's serial back from the (collapsed) history.
	hr := historyGet(t, srv, channel, "direction=forwards", "")
	var msgs []*protocol.Message
	decodeJSON(t, hr, &msgs)
	if len(msgs) == 0 {
		t.Fatal("publishOne: history empty after publish")
	}
	return msgs[len(msgs)-1].Serial
}

func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode JSON: %v (body %q)", err, body)
	}
}

// patch issues PATCH /channels/{channel}/messages/{serial}.
func patch(t *testing.T, srv *httptest.Server, channel, serial string, m *protocol.Message) *http.Response {
	t.Helper()
	body, _ := json.Marshal(m)
	return request(t, srv, http.MethodPatch, "/channels/"+channel+"/messages/"+serial, "application/json", body, true)
}

// versionSerialResponse is the PATCH response shape (RSL15e).
type versionSerialResponse struct {
	VersionSerial string `json:"versionSerial"`
}

func TestPatchUpdateReturnsVersionSerial(t *testing.T) {
	srv, _ := newTestServer(t)
	serial := publishOne(t, srv, "room", &protocol.Message{Name: "n", Data: "v1", ClientID: "alice"})

	resp := patch(t, srv, "room", serial, &protocol.Message{Action: protocol.MessageUpdate, Data: "v2"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PATCH status = %d, want 200", resp.StatusCode)
	}
	var got versionSerialResponse
	decodeJSON(t, resp, &got)
	if got.VersionSerial == "" || got.VersionSerial == serial {
		t.Errorf("versionSerial = %q, want a fresh version serial != identity %q", got.VersionSerial, serial)
	}

	// The merged content is observable via the single-message read.
	mr := request(t, srv, http.MethodGet, "/channels/room/messages/"+serial, "", nil, true)
	var m protocol.Message
	decodeJSON(t, mr, &m)
	if m.Serial != serial {
		t.Errorf("serial = %q, want stable identity %q", m.Serial, serial)
	}
	if m.Action != protocol.MessageUpdate || m.Data != "v2" {
		t.Errorf("got action=%v data=%v, want update/v2", m.Action, m.Data)
	}
	if m.Name != "n" {
		t.Errorf("name = %q, want carried-forward 'n'", m.Name)
	}
	if m.Version == nil || m.Version.Serial != got.VersionSerial {
		t.Errorf("version = %+v, want serial %q", m.Version, got.VersionSerial)
	}
}

func TestPatchDeleteThenSingleReadShowsTombstone(t *testing.T) {
	srv, _ := newTestServer(t)
	serial := publishOne(t, srv, "room", &protocol.Message{Data: "secret", ClientID: "alice"})

	if resp := patch(t, srv, "room", serial, &protocol.Message{Action: protocol.MessageDelete}); resp.StatusCode != http.StatusOK {
		t.Fatalf("PATCH delete status = %d, want 200", resp.StatusCode)
	}

	resp := request(t, srv, http.MethodGet, "/channels/room/messages/"+serial, "", nil, true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET message status = %d, want 200 (soft delete stays queryable)", resp.StatusCode)
	}
	var got protocol.Message
	decodeJSON(t, resp, &got)
	if got.Action != protocol.MessageDelete {
		t.Errorf("action = %v, want delete (tombstone)", got.Action)
	}
}

// TestPatchAppendAggregatesAndCollapses: PATCH appends concatenate onto
// the message; the single-message read returns the rolled-up aggregate,
// and the versions read collapses the append run rather than listing each
// delta (DESIGN.md §13.3, §13.4).
func TestPatchAppendAggregatesAndCollapses(t *testing.T) {
	srv, _ := newTestServer(t)
	serial := publishOne(t, srv, "room", &protocol.Message{Data: "a", ClientID: "alice"})

	for _, chunk := range []string{"b", "c", "d"} {
		if resp := patch(t, srv, "room", serial, &protocol.Message{Action: protocol.MessageAppend, Data: chunk}); resp.StatusCode != http.StatusOK {
			t.Fatalf("append %q: status %d, want 200", chunk, resp.StatusCode)
		}
	}

	// Single-message read returns the aggregate.
	mr := request(t, srv, http.MethodGet, "/channels/room/messages/"+serial, "", nil, true)
	var m protocol.Message
	decodeJSON(t, mr, &m)
	if m.Data != "abcd" {
		t.Errorf("aggregate data = %v, want abcd", m.Data)
	}
	if m.Alt != nil {
		t.Errorf("client-facing read leaked internal alt carrier: %+v", m.Alt)
	}

	// Versions read collapses the append run: create + aggregate = 2.
	vr := versionsGet(t, srv, "room", serial, "direction=forwards")
	var all []*protocol.Message
	decodeJSON(t, vr, &all)
	if len(all) != 2 {
		t.Fatalf("versions = %d, want 2 (create + collapsed aggregate)", len(all))
	}
	if all[1].Data != "abcd" {
		t.Errorf("collapsed aggregate data = %v, want abcd", all[1].Data)
	}
}

// TestPatchAppendIncompatibleRejected: appending a value whose type
// cannot concatenate onto the current data is a 400 (DESIGN.md §13.3).
func TestPatchAppendIncompatibleRejected(t *testing.T) {
	srv, _ := newTestServer(t)
	serial := publishOne(t, srv, "room", &protocol.Message{Data: "text", ClientID: "alice"})
	resp := patch(t, srv, "room", serial, &protocol.Message{Action: protocol.MessageAppend, Data: 42})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("incompatible append status = %d, want 400", resp.StatusCode)
	}
}

func TestGetSingleMessageNotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := request(t, srv, http.MethodGet, "/channels/room/messages/00000000000001-000@nope000000:000", "", nil, true)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestPatchTargetNotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := patch(t, srv, "room", "00000000000001-000@nope000000:000", &protocol.Message{Action: protocol.MessageUpdate, Data: "x"})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestPatchRejectsNonMutationAction(t *testing.T) {
	srv, _ := newTestServer(t)
	serial := publishOne(t, srv, "room", &protocol.Message{Data: "v1"})
	resp := patch(t, srv, "room", serial, &protocol.Message{Action: protocol.MessageCreate, Data: "v2"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (create not allowed on PATCH)", resp.StatusCode)
	}
}

func TestVersionsListPaginates(t *testing.T) {
	srv, _ := newTestServer(t)
	serial := publishOne(t, srv, "room", &protocol.Message{Data: "v1", ClientID: "alice"})
	for _, d := range []string{"v2", "v3"} {
		if resp := patch(t, srv, "room", serial, &protocol.Message{Action: protocol.MessageUpdate, Data: d}); resp.StatusCode != http.StatusOK {
			t.Fatalf("patch %s: status %d", d, resp.StatusCode)
		}
	}

	// Full forwards list: create + 2 updates = 3 versions.
	resp := versionsGet(t, srv, "room", serial, "direction=forwards")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("versions status = %d, want 200", resp.StatusCode)
	}
	var all []*protocol.Message
	decodeJSON(t, resp, &all)
	if len(all) != 3 {
		t.Fatalf("versions = %d, want 3 (create + 2 updates)", len(all))
	}
	for _, v := range all {
		if v.Serial != serial {
			t.Errorf("version serial = %q, want stable identity %q", v.Serial, serial)
		}
	}

	// Page 1 with limit=2 carries a rel="next" Link.
	resp = versionsGet(t, srv, "room", serial, "direction=forwards&limit=2")
	var page1 []*protocol.Message
	decodeJSON(t, resp, &page1)
	if len(page1) != 2 {
		t.Fatalf("page1 = %d, want 2", len(page1))
	}
	next := nextLink(t, resp)
	if next == "" {
		t.Fatal("no rel=next link on a limited versions page")
	}

	// Follow it (it is an absolute path on the same server).
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+next, nil)
	req.SetBasicAuth("app.key", "secret")
	resp2, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("follow next: %v", err)
	}
	t.Cleanup(func() { resp2.Body.Close() })
	var page2 []*protocol.Message
	decodeJSON(t, resp2, &page2)
	if len(page2) != 1 {
		t.Fatalf("page2 = %d, want 1 (the remaining version)", len(page2))
	}
	if storageVersionSerial(page2[0]) == storageVersionSerial(page1[1]) {
		t.Error("page2 repeated the page1 boundary version (cursor not excluded)")
	}
}

func TestVersionsNotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := versionsGet(t, srv, "room", "00000000000001-000@nope000000:000", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestCollapsedHistoryShowsLatest(t *testing.T) {
	srv, _ := newTestServer(t)
	serial := publishOne(t, srv, "room", &protocol.Message{Data: "v1", ClientID: "alice"})
	if resp := patch(t, srv, "room", serial, &protocol.Message{Action: protocol.MessageUpdate, Data: "v2"}); resp.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d", resp.StatusCode)
	}

	hr := historyGet(t, srv, "room", "direction=forwards", "")
	var msgs []*protocol.Message
	decodeJSON(t, hr, &msgs)
	if len(msgs) != 1 {
		t.Fatalf("collapsed history = %d messages, want 1", len(msgs))
	}
	if msgs[0].Data != "v2" {
		t.Errorf("collapsed data = %v, want v2 (latest)", msgs[0].Data)
	}
}

func TestPatchAndReadMsgpack(t *testing.T) {
	srv, _ := newTestServer(t)
	serial := publishOne(t, srv, "room", &protocol.Message{Data: "v1"})

	// PATCH with msgpack body + msgpack Accept.
	body, _ := msgpack.Marshal(&protocol.Message{Action: protocol.MessageUpdate, Data: "v2"})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPatch, srv.URL+"/channels/room/messages/"+serial, bytesReader(body))
	req.SetBasicAuth("app.key", "secret")
	req.Header.Set("Content-Type", "application/x-msgpack")
	req.Header.Set("Accept", "application/x-msgpack")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("PATCH msgpack: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-msgpack" {
		t.Errorf("Content-Type = %q, want application/x-msgpack", ct)
	}
	raw, _ := io.ReadAll(resp.Body)
	var got struct {
		VersionSerial string `msgpack:"versionSerial"`
	}
	if err := msgpack.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode msgpack: %v", err)
	}
	if got.VersionSerial == "" {
		t.Fatalf("msgpack mutation result has empty versionSerial")
	}

	// Read back the merged version via msgpack single-message read.
	mreq, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/channels/room/messages/"+serial, nil)
	mreq.SetBasicAuth("app.key", "secret")
	mreq.Header.Set("Accept", "application/x-msgpack")
	mresp, err := srv.Client().Do(mreq)
	if err != nil {
		t.Fatalf("GET msgpack: %v", err)
	}
	t.Cleanup(func() { mresp.Body.Close() })
	mraw, _ := io.ReadAll(mresp.Body)
	var m protocol.Message
	if err := msgpack.Unmarshal(mraw, &m); err != nil {
		t.Fatalf("decode msgpack message: %v", err)
	}
	if m.Data != "v2" || m.Action != protocol.MessageUpdate {
		t.Errorf("msgpack merged version = %+v, want update/v2", m)
	}
}

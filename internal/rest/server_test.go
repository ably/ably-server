package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

const testKey = "app.key:secret"

// newTestServer builds an httptest.Server wrapping our REST handler
// with a known API key. The returned Manager is the same one the
// server is wired with, so tests can attach streams and observe the
// effects of REST publishes.
func newTestServer(t *testing.T) (*httptest.Server, *core.Manager) {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rs := NewServer([]auth.APIKey{parsed}, manager, slog.New(slog.DiscardHandler), nil, nil, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /channels/{name}/messages", rs.HandlePublish)
	mux.HandleFunc("GET /channels/{name}/messages", rs.HandleHistory)
	mux.HandleFunc("GET /channels/{name}/history", rs.HandleHistory)
	mux.HandleFunc("PATCH /channels/{name}/messages/{serial}", rs.HandleMutate)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}", rs.HandleMessage)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}/versions", rs.HandleMessageVersions)
	mux.HandleFunc("GET /channels/{name}/presence", rs.HandlePresence)
	mux.HandleFunc("GET /channels/{name}/presence/history", rs.HandlePresenceHistory)
	mux.HandleFunc("GET /stats", rs.HandleStats)
	mux.HandleFunc("GET /time", rs.HandleTime)
	mux.HandleFunc("GET /healthz", rs.HandleHealthz)
	mux.HandleFunc("GET /readyz", rs.HandleReadyz)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, manager
}

// attachStream resolves the channel and attaches a Stream using a
// short-lived context — keeps the publish-observation tests compact.
func attachStream(t *testing.T, manager *core.Manager, name string) *core.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch, err := manager.GetChannel(ctx, name)
	if err != nil {
		t.Fatalf("GetChannel %q: %v", name, err)
	}
	stream, err := ch.Attach(ctx)
	if err != nil {
		t.Fatalf("Attach %q: %v", name, err)
	}
	return stream
}

// request issues a request to srv with the test key in Basic auth
// (unless authed is false) and returns the response.
func request(t *testing.T, srv *httptest.Server, method, path, contentType string, body []byte, authed bool) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if authed {
		req.SetBasicAuth("app.key", "secret")
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestPublishJSONSingleMessage(t *testing.T) {
	srv, manager := newTestServer(t)

	body, _ := json.Marshal(&protocol.Message{Name: "greet", Data: "hello"})
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	// Verify the publish landed on the channel by attaching a stream
	// and observing the next message.
	stream := attachStream(t, manager, "foo")
	go func() {
		// In case the publish reaches the channel before the stream
		// observed it (unlikely since Attach captures tail post-publish),
		// publish a sentinel; not needed if the timing is fine.
	}()
	// The publish happened before Attach so we won't see it via Next.
	// Instead, walk the channel via a fresh publish + Next loop is
	// overkill — verify by Append-then-attach ordering: do another
	// publish and ensure both arrived in order by checking the second
	// via Next.
	_ = stream

	body2, _ := json.Marshal(&protocol.Message{Name: "greet", Data: "world"})
	resp2 := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body2, true)
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp2.StatusCode, http.StatusCreated)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(cm.Messages) != 1 {
		t.Fatalf("Messages length = %d, want 1", len(cm.Messages))
	}
	got := cm.Messages[0]
	if got.Name != "greet" || got.Data != "world" {
		t.Errorf("got = %+v, want greet/world", got)
	}
}

func TestPublishJSONArrayBody(t *testing.T) {
	srv, manager := newTestServer(t)
	stream := attachStream(t, manager, "foo")

	msgs := []*protocol.Message{
		{Name: "a", Data: "1"},
		{Name: "b", Data: "2"},
	}
	body, _ := json.Marshal(msgs)
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	// A single publish (array body) lands as one ChannelMessage
	// carrying both messages.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(cm.Messages) != len(msgs) {
		t.Fatalf("Messages length = %d, want %d", len(cm.Messages), len(msgs))
	}
	for i, want := range msgs {
		got := cm.Messages[i]
		if got.Name != want.Name || got.Data != want.Data {
			t.Errorf("msg %d = %+v, want %+v", i, got, want)
		}
	}
}

// publishResponseBody is a local decode target mirroring the server's
// publishResponse — the tests decode the wire body independently to pin
// the {channel, messageId} shape (Ably RSL1) for both formats.
type publishResponseBody struct {
	Channel   string `json:"channel"   msgpack:"channel"`
	MessageID string `json:"messageId" msgpack:"messageId"`
}

func TestPublishResponseBodyJSON(t *testing.T) {
	srv, manager := newTestServer(t)
	stream := attachStream(t, manager, "foo")

	body, _ := json.Marshal(&protocol.Message{Name: "greet", Data: "hi"})
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	raw, _ := io.ReadAll(resp.Body)
	var got publishResponseBody
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode JSON response: %v (body %q)", err, raw)
	}
	if got.Channel != "foo" {
		t.Errorf("channel = %q, want %q", got.Channel, "foo")
	}
	if got.MessageID == "" {
		t.Fatal("messageId is empty")
	}

	// AC#3: messageId is the id carried on the delivered MESSAGE for the
	// same publish (the value a WS subscriber would see).
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got.MessageID != cm.Messages[0].ID {
		t.Errorf("messageId = %q, want delivered Message.ID %q", got.MessageID, cm.Messages[0].ID)
	}
	// The shape is "<batchID>:0" for the first message (Ably's e.g. "TojWzTkLiH:0").
	if !strings.HasSuffix(got.MessageID, ":0") {
		t.Errorf("messageId = %q, want a trailing \":0\" (first-message batch id)", got.MessageID)
	}
}

func TestPublishResponseBodyMsgpack(t *testing.T) {
	srv, manager := newTestServer(t)
	stream := attachStream(t, manager, "foo")

	body, err := msgpack.Marshal(&protocol.Message{Name: "greet", Data: "hi"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/channels/foo/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-msgpack")
	req.Header.Set("Accept", "application/x-msgpack")
	req.SetBasicAuth("app.key", "secret")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-msgpack" {
		t.Errorf("Content-Type = %q, want application/x-msgpack", ct)
	}

	raw, _ := io.ReadAll(resp.Body)
	var got publishResponseBody
	if err := msgpack.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode msgpack response: %v", err)
	}
	if got.Channel != "foo" {
		t.Errorf("channel = %q, want %q", got.Channel, "foo")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got.MessageID != cm.Messages[0].ID {
		t.Errorf("messageId = %q, want delivered Message.ID %q", got.MessageID, cm.Messages[0].ID)
	}
}

func TestPublishResponseMessageIDIsFirstOfBatch(t *testing.T) {
	srv, manager := newTestServer(t)
	stream := attachStream(t, manager, "foo")

	msgs := []*protocol.Message{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	body, _ := json.Marshal(msgs)
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	raw, _ := io.ReadAll(resp.Body)
	var got publishResponseBody
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// One publish (array body) yields exactly one messageId — the first
	// message's stamped id.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(cm.Messages) != 3 {
		t.Fatalf("delivered Messages = %d, want 3", len(cm.Messages))
	}
	if got.MessageID != cm.Messages[0].ID {
		t.Errorf("messageId = %q, want first message id %q", got.MessageID, cm.Messages[0].ID)
	}
}

func TestPublishMsgpackArrayBody(t *testing.T) {
	srv, manager := newTestServer(t)
	stream := attachStream(t, manager, "foo")

	msgs := []*protocol.Message{
		{Name: "a", Data: "1"},
		{Name: "b", Data: "2"},
		{Name: "c", Data: "3"},
	}
	body, err := msgpack.Marshal(msgs)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/x-msgpack", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	// A msgpack array body is one atomic publish: it lands as a single
	// ChannelMessage carrying every message, matching a WS MESSAGE frame's
	// messages[] (RSL1).
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(cm.Messages) != len(msgs) {
		t.Fatalf("Messages length = %d, want %d (array must land as one ChannelMessage)", len(cm.Messages), len(msgs))
	}
	for i, want := range msgs {
		got := cm.Messages[i]
		if got.Name != want.Name || got.Data != want.Data {
			t.Errorf("msg %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestPublishMsgpack(t *testing.T) {
	srv, manager := newTestServer(t)
	stream := attachStream(t, manager, "foo")

	body, err := msgpack.Marshal(&protocol.Message{Name: "ping", Data: "pong"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/x-msgpack", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(cm.Messages) != 1 {
		t.Fatalf("Messages length = %d, want 1", len(cm.Messages))
	}
	got := cm.Messages[0]
	if got.Name != "ping" || got.Data != "pong" {
		t.Errorf("got = %+v, want ping/pong", got)
	}
}

func TestPublishRejectsMissingCredentials(t *testing.T) {
	srv, _ := newTestServer(t)
	body, _ := json.Marshal(&protocol.Message{Data: "x"})
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body, false)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
		t.Errorf("WWW-Authenticate = %q, want Basic", got)
	}
}

func TestPublishRejectsWrongCredentials(t *testing.T) {
	srv, _ := newTestServer(t)
	body, _ := json.Marshal(&protocol.Message{Data: "x"})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/channels/foo/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("app.key", "wrong")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestPublishRejectsEmptyBody(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", nil, true)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestPublishRejectsUnsupportedContentType(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "text/xml", []byte("<x/>"), true)
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415", resp.StatusCode)
	}
}

func TestTime(t *testing.T) {
	srv, _ := newTestServer(t)
	before := time.Now().UnixMilli()
	resp := request(t, srv, http.MethodGet, "/time", "", nil, false)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	after := time.Now().UnixMilli()
	body, _ := io.ReadAll(resp.Body)
	var arr []int64
	if err := json.Unmarshal(body, &arr); err != nil {
		t.Fatalf("decode: %v (body %q)", err, body)
	}
	if len(arr) != 1 {
		t.Fatalf("len(arr) = %d, want 1", len(arr))
	}
	if arr[0] < before || arr[0] > after {
		t.Errorf("server time = %d, want in [%d, %d]", arr[0], before, after)
	}
}

func TestHealthzAndReadyzNoAuth(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		resp := request(t, srv, http.MethodGet, path, "", nil, false)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s status = %d, want 200", path, resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "ok" {
			t.Errorf("%s body = %q, want %q", path, body, "ok")
		}
	}
}

// fakePinger is a storage.Pinger stub for exercising HandleReadyz's
// cluster-mode dependency check without a real Postgres.
type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

// newTestServerWithReady is like newTestServer but wires ready as the
// Server's storage.Pinger, so tests can drive HandleReadyz's
// cluster-mode branch directly.
func newTestServerWithReady(t *testing.T, ready storage.Pinger) *httptest.Server {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rs := NewServer([]auth.APIKey{parsed}, manager, slog.New(slog.DiscardHandler), ready, nil, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", rs.HandleHealthz)
	mux.HandleFunc("GET /readyz", rs.HandleReadyz)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestReadyzClusterModeReachable(t *testing.T) {
	srv := newTestServerWithReady(t, fakePinger{})
	resp := request(t, srv, http.MethodGet, "/readyz", "", nil, false)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz status = %d, want 200", resp.StatusCode)
	}
}

func TestReadyzClusterModeUnreachable(t *testing.T) {
	srv := newTestServerWithReady(t, fakePinger{err: errors.New("dial tcp: connection refused")})

	resp := request(t, srv, http.MethodGet, "/readyz", "", nil, false)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/readyz status = %d, want 503", resp.StatusCode)
	}

	// /healthz stays dependency-free and unaffected.
	resp = request(t, srv, http.MethodGet, "/healthz", "", nil, false)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz status = %d, want 200", resp.StatusCode)
	}
}

// historyGet issues GET /channels/{channel}/messages with the test
// API key in Basic auth. accept may be empty (server defaults to
// JSON).
func historyGet(t *testing.T, srv *httptest.Server, channel, rawQuery, accept string) *http.Response {
	t.Helper()
	u := srv.URL + "/channels/" + channel + "/messages"
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
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

// publishBatch sends one POST with the given messages and fails the
// test on a non-201 response.
func publishBatch(t *testing.T, srv *httptest.Server, channel string, msgs []*protocol.Message) {
	t.Helper()
	body, err := json.Marshal(msgs)
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	resp := request(t, srv, http.MethodPost, "/channels/"+channel+"/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish batch: status = %d, want 201", resp.StatusCode)
	}
}

func decodeHistoryJSON(t *testing.T, resp *http.Response) []*protocol.Message {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out []*protocol.Message
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode body %q: %v", body, err)
	}
	return out
}

func messageNames(ms []*protocol.Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.Name
	}
	return out
}

func TestHistoryRequiresAuth(t *testing.T) {
	srv, _ := newTestServer(t)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/channels/foo/messages", nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestHistoryEmptyChannelReturnsEmptyArray(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := historyGet(t, srv, "foo", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "[]" {
		t.Errorf("body = %q, want %q", body, "[]")
	}
}

func TestHistoryDefaultsToBackwardsWithFullReversal(t *testing.T) {
	srv, _ := newTestServer(t)
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "a0"}, {Name: "a1"}, {Name: "a2"}})
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "b0"}, {Name: "b1"}})

	resp := historyGet(t, srv, "foo", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got := messageNames(decodeHistoryJSON(t, resp))
	want := []string{"b1", "b0", "a2", "a1", "a0"}
	if !equalStrings(got, want) {
		t.Errorf("default direction names = %v, want %v (Ably-verified full reversal)", got, want)
	}
}

func TestHistoryForwardsPreservesOrder(t *testing.T) {
	srv, _ := newTestServer(t)
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "a0"}, {Name: "a1"}, {Name: "a2"}})
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "b0"}, {Name: "b1"}})

	resp := historyGet(t, srv, "foo", "direction=forwards", "")
	got := messageNames(decodeHistoryJSON(t, resp))
	want := []string{"a0", "a1", "a2", "b0", "b1"}
	if !equalStrings(got, want) {
		t.Errorf("forwards names = %v, want %v", got, want)
	}
}

func TestHistoryLimitAndCursorTraversal(t *testing.T) {
	srv, _ := newTestServer(t)
	for i := range 5 {
		publishBatch(t, srv, "foo", []*protocol.Message{{Name: fmt.Sprintf("m%d", i)}})
	}

	// First page: backwards, limit 2 — newest two.
	resp := historyGet(t, srv, "foo", "limit=2", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got := messageNames(decodeHistoryJSON(t, resp))
	if !equalStrings(got, []string{"m4", "m3"}) {
		t.Fatalf("page1 names = %v, want [m4 m3]", got)
	}
	nextURL := nextLink(t, resp)
	if nextURL == "" {
		t.Fatal("page1 missing rel=next link")
	}

	// Follow the opaque next link verbatim (no parsing of cursor value).
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+nextURL, nil)
	req.SetBasicAuth("app.key", "secret")
	resp2, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("page2 do: %v", err)
	}
	t.Cleanup(func() { resp2.Body.Close() })
	got = messageNames(decodeHistoryJSON(t, resp2))
	if !equalStrings(got, []string{"m2", "m1"}) {
		t.Fatalf("page2 names = %v, want [m2 m1]", got)
	}
	nextURL2 := nextLink(t, resp2)
	if nextURL2 == "" {
		t.Fatal("page2 missing rel=next link")
	}

	// Final page: one message remaining; no further next link.
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+nextURL2, nil)
	req.SetBasicAuth("app.key", "secret")
	resp3, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("page3 do: %v", err)
	}
	t.Cleanup(func() { resp3.Body.Close() })
	got = messageNames(decodeHistoryJSON(t, resp3))
	if !equalStrings(got, []string{"m0"}) {
		t.Errorf("page3 names = %v, want [m0]", got)
	}
	if nextLink(t, resp3) != "" {
		t.Error("page3 should not have a rel=next link")
	}
}

func TestHistoryRejectsInvalidParams(t *testing.T) {
	srv, _ := newTestServer(t)
	cases := []string{
		"direction=sideways",
		"limit=abc",
		"limit=0",
		"limit=1001",
		"start=-1",
		"end=notanumber",
	}
	for _, raw := range cases {
		resp := historyGet(t, srv, "foo", raw, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%q: status = %d, want 400", raw, resp.StatusCode)
		}
	}
}

func TestHistoryMsgpackRoundTrip(t *testing.T) {
	srv, _ := newTestServer(t)
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "a"}, {Name: "b"}})

	resp := historyGet(t, srv, "foo", "direction=forwards", "application/x-msgpack")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-msgpack" {
		t.Errorf("Content-Type = %q, want application/x-msgpack", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	var got []*protocol.Message
	if err := msgpack.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode msgpack: %v", err)
	}
	if !equalStrings(messageNames(got), []string{"a", "b"}) {
		t.Errorf("names = %v, want [a b]", messageNames(got))
	}
}

func TestHistoryLimitSplitsAtomicBatch(t *testing.T) {
	// Ably's `limit` counts Messages and slices a multi-message
	// atomic publish across pages (verified empirically against the
	// live API).
	srv, _ := newTestServer(t)
	publishBatch(t, srv, "foo", []*protocol.Message{
		{Name: "m0"}, {Name: "m1"}, {Name: "m2"}, {Name: "m3"}, {Name: "m4"},
	})

	// Page 1: backwards, limit 2 → m4, m3.
	resp := historyGet(t, srv, "foo", "limit=2", "")
	got := messageNames(decodeHistoryJSON(t, resp))
	if !equalStrings(got, []string{"m4", "m3"}) {
		t.Fatalf("page1 names = %v, want [m4 m3]", got)
	}
	nextURL := nextLink(t, resp)
	if nextURL == "" {
		t.Fatal("expected a rel=next link with a mid-batch cursor")
	}
	// The cursor in the next link must look like a Message.Serial
	// (channelSerial:idx). It's URL-encoded inside the link.
	if !strings.Contains(nextURL, "%3A") && !strings.Contains(nextURL, ":") {
		t.Errorf("next link cursor not in Message.Serial form: %q", nextURL)
	}

	// Page 2: follow the opaque cursor.
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+nextURL, nil)
	req.SetBasicAuth("app.key", "secret")
	resp2, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("page2 do: %v", err)
	}
	t.Cleanup(func() { resp2.Body.Close() })
	got = messageNames(decodeHistoryJSON(t, resp2))
	if !equalStrings(got, []string{"m2", "m1"}) {
		t.Fatalf("page2 names = %v, want [m2 m1]", got)
	}
}

func TestHistoryLinkHeadersAlwaysIncludeFirstAndCurrent(t *testing.T) {
	srv, _ := newTestServer(t)
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "x"}})
	resp := historyGet(t, srv, "foo", "limit=10", "")
	link := strings.Join(resp.Header.Values("Link"), ", ")
	if !strings.Contains(link, `rel="current"`) {
		t.Errorf("Link missing rel=current: %q", link)
	}
	if !strings.Contains(link, `rel="first"`) {
		t.Errorf("Link missing rel=first: %q", link)
	}
	if strings.Contains(link, `rel="next"`) {
		t.Errorf("Link unexpectedly contains rel=next when HasMore=false: %q", link)
	}
}

// TestHistoryLinkHeadersRelativeAndSeparate pins the wire shape Ably SDKs
// require (TASK-76): each rel is its own Link header line (SDKs parse each
// Header["Link"] element with a single-match regexp), and the link URL is
// relative to the resource — the request path's final segment plus query,
// never an absolute path (SDKs resolve it against path.Dir(requestPath)).
func TestHistoryLinkHeadersRelativeAndSeparate(t *testing.T) {
	srv, _ := newTestServer(t)
	publishBatch(t, srv, "foo", []*protocol.Message{
		{Name: "m0"}, {Name: "m1"}, {Name: "m2"},
	})

	for _, tc := range []struct {
		name, path, base string
	}{
		{"messages", "/channels/foo/messages?limit=1", "messages"},
		{"history-alias", "/channels/foo/history?limit=1", "history"},
		{"presence-history", "/channels/foo/presence/history?limit=1", "history"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+tc.path, nil)
			req.SetBasicAuth("app.key", "secret")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			t.Cleanup(func() { resp.Body.Close() })

			values := resp.Header.Values("Link")
			// current + first (+ next when there is more) — each on its own line.
			if len(values) < 2 {
				t.Fatalf("want >=2 separate Link headers, got %d: %v", len(values), values)
			}
			for _, v := range values {
				if strings.Count(v, "rel=") != 1 {
					t.Errorf("Link header line carries multiple rels: %q", v)
				}
				// Extract the URL and assert it is relative to the resource.
				start, end := strings.Index(v, "<"), strings.Index(v, ">")
				if start != 0 || end < 0 {
					t.Fatalf("malformed Link entry: %q", v)
				}
				linkURL := v[1:end]
				if strings.HasPrefix(linkURL, "/") || strings.HasPrefix(linkURL, "http") {
					t.Errorf("Link URL must be relative to the resource, got %q", linkURL)
				}
				if !strings.HasPrefix(linkURL, tc.base) {
					t.Errorf("Link URL %q does not start with resource segment %q", linkURL, tc.base)
				}
			}
		})
	}
}

// nextLink extracts the rel="next" entry from resp's Link headers and
// resolves it against the request URL, returning a server-root-relative
// path (e.g. "/channels/foo/messages?from=..."), or "" if no such entry
// is present. Links are emitted as separate Link header lines and are
// relative to the request resource, matching how Ably SDKs resolve them
// (path.Dir(requestPath) + link) — so this mirrors that resolution.
func nextLink(t *testing.T, resp *http.Response) string {
	t.Helper()
	for _, header := range resp.Header.Values("Link") {
		for part := range strings.SplitSeq(header, ",") {
			part = strings.TrimSpace(part)
			if !strings.Contains(part, `rel="next"`) {
				continue
			}
			// Format: <url>; rel="next"
			end := strings.Index(part, ">")
			if !strings.HasPrefix(part, "<") || end < 0 {
				t.Fatalf("malformed Link entry: %q", part)
			}
			ref, err := url.Parse(part[1:end])
			if err != nil {
				t.Fatalf("parse link %q: %v", part, err)
			}
			return resp.Request.URL.ResolveReference(ref).RequestURI()
		}
	}
	return ""
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, s := range a {
		if s != b[i] {
			return false
		}
	}
	return true
}

package rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
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
	return newTestServerWithKeys(t, parsed)
}

// newTestServerWithKeys is newTestServer with caller-supplied keys, so
// tests can exercise per-key capabilities.
func newTestServerWithKeys(t *testing.T, keys ...auth.APIKey) (*httptest.Server, *core.Manager) {
	t.Helper()
	return newTestServerWithResolver(t, nil, keys...)
}

// newTestServerWithResolver is newTestServerWithKeys with a caller-supplied
// ConnectionResolver, so publish-on-behalf (connectionKey) resolution can be
// exercised without a live realtime endpoint.
func newTestServerWithResolver(t *testing.T, conns ConnectionResolver, keys ...auth.APIKey) (*httptest.Server, *core.Manager) {
	t.Helper()
	manager := core.NewManager(memory.New(memory.Options{}))
	rs := NewServer(keys, manager, logging.New(slog.DiscardHandler), nil, nil, nil, conns)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /channels/{name}/messages", rs.HandlePublish)
	mux.HandleFunc("GET /channels/{name}/messages", rs.HandleHistory)
	mux.HandleFunc("GET /channels/{name}/history", rs.HandleHistory)
	mux.HandleFunc("PATCH /channels/{name}/messages/{serial}", rs.HandleMutate)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}", rs.HandleMessage)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}/versions", rs.HandleMessageVersions)
	mux.HandleFunc("POST /channels/{name}/messages/{serial}/annotations", rs.HandlePublishAnnotation)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}/annotations", rs.HandleListAnnotations)
	mux.HandleFunc("GET /channels/{name}/presence", rs.HandlePresence)
	mux.HandleFunc("GET /channels/{name}/presence/history", rs.HandlePresenceHistory)
	mux.HandleFunc("GET /stats", rs.HandleStats)
	mux.HandleFunc("POST /stats", rs.HandlePostStats)
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
// the {channel, messageId, serials} shape (Ably RSL1/RSL1n) for both
// formats.
type publishResponseBody struct {
	Channel   string   `json:"channel"   msgpack:"channel"`
	MessageID string   `json:"messageId" msgpack:"messageId"`
	Serials   []string `json:"serials"   msgpack:"serials"`
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

	// RSL1n: serials carries one entry per published message, in batch
	// order, each the message's stable identity Serial (the value the SDK's
	// PublishWithResult surfaces and then uses to address the message).
	if len(got.Serials) != 3 {
		t.Fatalf("serials = %d, want 3 (one per message)", len(got.Serials))
	}
	for i, s := range got.Serials {
		if s == "" {
			t.Errorf("serials[%d] is empty", i)
		}
		if s != cm.Messages[i].Serial {
			t.Errorf("serials[%d] = %q, want delivered Message.Serial %q", i, s, cm.Messages[i].Serial)
		}
	}
}

// TestPublishSerialAddressesMessage pins the end-to-end contract the SDK's
// PublishWithResult relies on: the serial returned in the publish response
// is the value that addresses that message via GET .../messages/{serial}.
func TestPublishSerialAddressesMessage(t *testing.T) {
	srv, _ := newTestServer(t)

	body, _ := json.Marshal(&protocol.Message{Name: "greet", Data: "hi"})
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	raw, _ := io.ReadAll(resp.Body)
	var got publishResponseBody
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Serials) != 1 || got.Serials[0] == "" {
		t.Fatalf("serials = %v, want a single non-empty serial", got.Serials)
	}

	// The returned serial must be directly addressable.
	mr := request(t, srv, http.MethodGet, "/channels/foo/messages/"+got.Serials[0], "", nil, true)
	if mr.StatusCode != http.StatusOK {
		t.Fatalf("GET by returned serial status = %d, want 200", mr.StatusCode)
	}
	var m protocol.Message
	decodeJSON(t, mr, &m)
	if m.Serial != got.Serials[0] || m.Data != "hi" {
		t.Errorf("read message = {serial:%q data:%v}, want {serial:%q data:hi}", m.Serial, m.Data, got.Serials[0])
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

// TestPublishRejectsNonConformingBatchIDs pins RSL1k3: a
// multi-message publish whose client-supplied ids don't follow the
// "<batchID>:<idx>" shape is rejected with an Ably 40031 error body, so the
// SDK surfaces the code rather than defaulting a bare 400 to 40000.
func TestPublishRejectsNonConformingBatchIDs(t *testing.T) {
	srv, _ := newTestServer(t)
	body, _ := json.Marshal([]*protocol.Message{
		{ID: "dup", Data: "a"},
		{ID: "dup", Data: "b"},
		{ID: "dup", Data: "c"},
	})
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Ably-Errorcode"); got != "40031" {
		t.Errorf("X-Ably-Errorcode = %q, want 40031", got)
	}
	raw, _ := io.ReadAll(resp.Body)
	var body2 errorResponse
	if err := json.Unmarshal(raw, &body2); err != nil {
		t.Fatalf("decode error body %q: %v", raw, err)
	}
	if body2.Error == nil || body2.Error.Code != 40031 {
		t.Errorf("error body = %+v, want code 40031", body2.Error)
	}
}

// TestPublishIdempotentDuplicateReturnsOnce pins that a repeated publish
// carrying the same batch id is de-duplicated: the second POST does not add
// a second message to history (server-side idempotency, §6/§8).
func TestPublishIdempotentDuplicateReturnsOnce(t *testing.T) {
	srv, _ := newTestServer(t)
	body, _ := json.Marshal(&protocol.Message{ID: "fixed-batch-id", Data: "hello"})
	for i := 0; i < 3; i++ {
		resp := request(t, srv, http.MethodPost, "/channels/idem/messages", "application/json", body, true)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("publish %d: status = %d, want 201", i, resp.StatusCode)
		}
	}
	resp := historyGet(t, srv, "idem", "limit=100", "")
	got := decodeHistoryJSON(t, resp)
	if len(got) != 1 {
		t.Fatalf("history = %d messages, want 1 (idempotent de-dup)", len(got))
	}
}

func TestPublishRejectsUnsupportedContentType(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := request(t, srv, http.MethodPost, "/channels/foo/messages", "text/xml", []byte("<x/>"), true)
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415", resp.StatusCode)
	}
}

// TestTime pins the /time endpoint: GET /time honours the Accept header, returning
// a msgpack-encoded [millis] array with the msgpack Content-Type on
// request, and JSON by default.
func TestTime(t *testing.T) {
	srv, _ := newTestServer(t)

	for _, tc := range []struct {
		name, accept, wantCT string
		decode               func([]byte, *[]int64) error
	}{
		{"json default", "", "application/json", func(b []byte, v *[]int64) error { return json.Unmarshal(b, v) }},
		{"msgpack", "application/x-msgpack", "application/x-msgpack", func(b []byte, v *[]int64) error { return msgpack.Unmarshal(b, v) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := time.Now().UnixMilli()

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/time", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			t.Cleanup(func() { resp.Body.Close() })

			after := time.Now().UnixMilli()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if ct := resp.Header.Get("Content-Type"); ct != tc.wantCT {
				t.Errorf("Content-Type = %q, want %q", ct, tc.wantCT)
			}

			body, _ := io.ReadAll(resp.Body)
			var arr []int64
			if err := tc.decode(body, &arr); err != nil {
				t.Fatalf("decode: %v (body %q)", err, body)
			}
			if len(arr) != 1 {
				t.Fatalf("len(arr) = %d, want 1", len(arr))
			}
			if arr[0] < before || arr[0] > after {
				t.Errorf("server time = %d, want in [%d, %d]", arr[0], before, after)
			}
		})
	}
}

// TestHandleNotFoundAblyError pins the unknown-resource 404:
// an Ably ErrorInfo body carrying code 40400 plus the X-Ably-Errorcode /
// X-Ably-Errormessage headers SDKs read, in the Accept format.
func TestHandleNotFoundAblyError(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rs := NewServer([]auth.APIKey{parsed}, manager, logging.New(slog.DiscardHandler), nil, nil, nil, nil)

	for _, tc := range []struct {
		name, accept, wantCT string
		decode               func([]byte, *errorResponse) error
	}{
		{"json default", "", "application/json", func(b []byte, v *errorResponse) error { return json.Unmarshal(b, v) }},
		{"msgpack", "application/x-msgpack", "application/x-msgpack", func(b []byte, v *errorResponse) error { return msgpack.Unmarshal(b, v) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/does/not/exist", nil)
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			rec := httptest.NewRecorder()
			rs.HandleNotFound(rec, req)

			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", rec.Code)
			}
			if ct := rec.Header().Get("Content-Type"); ct != tc.wantCT {
				t.Errorf("Content-Type = %q, want %q", ct, tc.wantCT)
			}
			if got := rec.Header().Get("X-Ably-Errorcode"); got != "40400" {
				t.Errorf("X-Ably-Errorcode = %q, want 40400", got)
			}
			if rec.Header().Get("X-Ably-Errormessage") == "" {
				t.Error("X-Ably-Errormessage header is empty")
			}
			var body errorResponse
			if err := tc.decode(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body.Error == nil {
				t.Fatal("error envelope missing")
			}
			if body.Error.Code != 40400 {
				t.Errorf("body error code = %d, want 40400", body.Error.Code)
			}
			if body.Error.StatusCode != http.StatusNotFound {
				t.Errorf("body error statusCode = %d, want 404", body.Error.StatusCode)
			}
			if want := "https://help.ably.io/error/40400"; body.Error.HRef != want {
				t.Errorf("body error href = %q, want %q", body.Error.HRef, want)
			}
		})
	}
}

// TestErrorInfoEnvelopeAcrossPaths pins the ErrorInfo envelope: representative REST error
// paths return the Ably ErrorInfo envelope with code, statusCode, message
// and href, plus the X-Ably-Errorcode / X-Ably-Errormessage headers SDKs
// read. Bad-channel-name and invalid-body are exercised in both the JSON
// and msgpack Accept formats.
func TestErrorInfoEnvelopeAcrossPaths(t *testing.T) {
	srv, _ := newTestServer(t)

	decodeJSON := func(b []byte, v *errorResponse) error { return json.Unmarshal(b, v) }
	decodeMsgpack := func(b []byte, v *errorResponse) error { return msgpack.Unmarshal(b, v) }

	for _, tc := range []struct {
		name        string
		method      string
		path        string
		contentType string
		accept      string
		body        []byte
		decode      func([]byte, *errorResponse) error
		wantCT      string
		wantStatus  int
		wantCode    int
	}{
		{
			name: "bad channel name json", method: http.MethodPost,
			path: "/channels/%20bad/messages", contentType: "application/json",
			accept: "application/json", body: []byte(`{"data":"x"}`),
			decode: decodeJSON, wantCT: "application/json",
			wantStatus: http.StatusBadRequest, wantCode: 40010,
		},
		{
			name: "bad channel name msgpack", method: http.MethodPost,
			path: "/channels/%20bad/messages", contentType: "application/json",
			accept: "application/x-msgpack", body: []byte(`{"data":"x"}`),
			decode: decodeMsgpack, wantCT: "application/x-msgpack",
			wantStatus: http.StatusBadRequest, wantCode: 40010,
		},
		{
			name: "invalid body json", method: http.MethodPost,
			path: "/channels/foo/messages", contentType: "application/json",
			accept: "application/json", body: []byte("{not json"),
			decode: decodeJSON, wantCT: "application/json",
			wantStatus: http.StatusBadRequest, wantCode: 40001,
		},
		{
			name: "invalid body msgpack", method: http.MethodPost,
			path: "/channels/foo/messages", contentType: "application/json",
			accept: "application/x-msgpack", body: []byte("{not json"),
			decode: decodeMsgpack, wantCT: "application/x-msgpack",
			wantStatus: http.StatusBadRequest, wantCode: 40001,
		},
		{
			name: "unsupported content type", method: http.MethodPost,
			path: "/channels/foo/messages", contentType: "text/xml",
			accept: "application/json", body: []byte("<x/>"),
			decode: decodeJSON, wantCT: "application/json",
			wantStatus: http.StatusUnsupportedMediaType, wantCode: 40004,
		},
		{
			name: "unacceptable accept", method: http.MethodGet,
			path: "/channels/foo/messages", accept: "text/xml",
			decode: decodeJSON, wantCT: "application/json",
			wantStatus: http.StatusNotAcceptable, wantCode: 40004,
		},
		{
			name: "unknown message", method: http.MethodGet,
			path: "/channels/foo/messages/nope", accept: "application/json",
			decode: decodeJSON, wantCT: "application/json",
			wantStatus: http.StatusNotFound, wantCode: 40400,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), tc.method, srv.URL+tc.path, bytes.NewReader(tc.body))
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			req.SetBasicAuth("app.key", "secret")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if ct := resp.Header.Get("Content-Type"); ct != tc.wantCT {
				t.Errorf("Content-Type = %q, want %q", ct, tc.wantCT)
			}
			if got := resp.Header.Get("X-Ably-Errorcode"); got != strconv.Itoa(tc.wantCode) {
				t.Errorf("X-Ably-Errorcode = %q, want %d", got, tc.wantCode)
			}
			if resp.Header.Get("X-Ably-Errormessage") == "" {
				t.Error("X-Ably-Errormessage header is empty")
			}
			raw, _ := io.ReadAll(resp.Body)
			var envelope errorResponse
			if err := tc.decode(raw, &envelope); err != nil {
				t.Fatalf("decode error body %q: %v", raw, err)
			}
			if envelope.Error == nil {
				t.Fatal("error envelope missing")
			}
			if envelope.Error.Code != tc.wantCode {
				t.Errorf("body error code = %d, want %d", envelope.Error.Code, tc.wantCode)
			}
			if envelope.Error.StatusCode != tc.wantStatus {
				t.Errorf("body error statusCode = %d, want %d", envelope.Error.StatusCode, tc.wantStatus)
			}
			if envelope.Error.Message == "" {
				t.Error("body error message is empty")
			}
			if want := fmt.Sprintf("https://help.ably.io/error/%d", tc.wantCode); envelope.Error.HRef != want {
				t.Errorf("body error href = %q, want %q", envelope.Error.HRef, want)
			}
		})
	}
}

// TestPostStatsIsPromptNoOp pins the POST /stats stub: SDK test
// flows write stats before reading them, and the write path treats a
// non-2xx as an error whose body it reads — a 404 left some SDKs' REST
// test clients (e.g. ably-go's TestRestClient) blocked. POST /stats must answer promptly with an empty
// 201 (authenticated like the GET), draining the request body.
func TestPostStatsIsPromptNoOp(t *testing.T) {
	srv, _ := newTestServer(t)
	body, _ := json.Marshal([]map[string]any{{"a": 1}, {"b": 2}})
	done := make(chan *http.Response, 1)
	go func() {
		done <- request(t, srv, http.MethodPost, "/stats", "application/json", body, true)
	}()
	select {
	case resp := <-done:
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("status = %d, want 201", resp.StatusCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("POST /stats did not respond promptly (hang)")
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
	rs := NewServer([]auth.APIKey{parsed}, manager, logging.New(slog.DiscardHandler), ready, nil, nil, nil)
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

// TestHistoryFromSerialBoundsToAttachPoint mirrors ably-js's
// RealtimeChannel.history({untilAttach: true}): it publishes some
// messages, captures the create channelSerial of the last one (the
// stand-in for an ATTACHED ChannelSerial / attachSerial), publishes
// more, then asserts fromSerial=<that point> returns only the messages
// up to and including it.
func TestHistoryFromSerialBoundsToAttachPoint(t *testing.T) {
	srv, _ := newTestServer(t)
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "pre-1"}, {Name: "pre-2"}})
	attachSerial := publishOneAndChannelSerial(t, srv, "foo", "pre-3")
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "post-1"}, {Name: "post-2"}})

	resp := historyGet(t, srv, "foo", "fromSerial="+url.QueryEscape(attachSerial), "")
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, body)
	}
	got := messageNames(decodeHistoryJSON(t, resp))
	want := []string{"pre-3", "pre-2", "pre-1"}
	if !equalStrings(got, want) {
		t.Errorf("names = %v, want %v", got, want)
	}
}

// TestHistoryFromSerialAcceptsSnakeCaseAlias locks in that from_serial
// (the literal query key ably-js puts on the wire — see
// realtimechannel.ts's `params.from_serial = this.properties.attachSerial`)
// is honoured the same as the camelCase fromSerial spelling, matching
// the reference server's dual naming.
func TestHistoryFromSerialAcceptsSnakeCaseAlias(t *testing.T) {
	srv, _ := newTestServer(t)
	attachSerial := publishOneAndChannelSerial(t, srv, "foo", "pre-1")
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "post-1"}})

	resp := historyGet(t, srv, "foo", "from_serial="+url.QueryEscape(attachSerial), "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got := messageNames(decodeHistoryJSON(t, resp))
	if !equalStrings(got, []string{"pre-1"}) {
		t.Errorf("names = %v, want [pre-1]", got)
	}
}

// TestHistoryFromSerialComposesWithDirectionAndLimit checks the bound
// holds in both scan directions and still composes with limit-based
// paging (AC #1: "in both directions").
func TestHistoryFromSerialComposesWithDirectionAndLimit(t *testing.T) {
	srv, _ := newTestServer(t)
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "pre-1"}, {Name: "pre-2"}})
	attachSerial := publishOneAndChannelSerial(t, srv, "foo", "pre-3")
	publishBatch(t, srv, "foo", []*protocol.Message{{Name: "post-1"}})

	resp := historyGet(t, srv, "foo", "fromSerial="+url.QueryEscape(attachSerial)+"&direction=forwards&limit=2", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got := messageNames(decodeHistoryJSON(t, resp))
	if !equalStrings(got, []string{"pre-1", "pre-2"}) {
		t.Errorf("page1 names = %v, want [pre-1 pre-2]", got)
	}
	nextURL := nextLink(t, resp)
	if nextURL == "" {
		t.Fatal("expected a rel=next link (pre-3 still to come)")
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+nextURL, nil)
	req.SetBasicAuth("app.key", "secret")
	resp2, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("page2 do: %v", err)
	}
	t.Cleanup(func() { resp2.Body.Close() })
	got = messageNames(decodeHistoryJSON(t, resp2))
	if !equalStrings(got, []string{"pre-3"}) {
		t.Fatalf("page2 names = %v, want [pre-3] (post-1 excluded by fromSerial)", got)
	}
	if nextLink(t, resp2) != "" {
		t.Error("page2 should not have a rel=next link (fromSerial excludes post-1)")
	}
}

// publishOneAndChannelSerial publishes a single message and returns the
// create channelSerial (storage.CreateChannelSerial of the returned
// Message.Serial) it landed at — a stand-in for the ATTACHED
// ChannelSerial ably-js captures as attachSerial.
func publishOneAndChannelSerial(t *testing.T, srv *httptest.Server, channel, name string) string {
	t.Helper()
	body, _ := json.Marshal(&protocol.Message{Name: name})
	resp := request(t, srv, http.MethodPost, "/channels/"+channel+"/messages", "application/json", body, true)
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("publish %q: status = %d, want 201 (body %s)", name, resp.StatusCode, b)
	}
	var got publishResponseBody
	decodeJSON(t, resp, &got)
	if len(got.Serials) != 1 || got.Serials[0] == "" {
		t.Fatalf("publish %q: serials = %v, want a single non-empty serial", name, got.Serials)
	}
	return storage.CreateChannelSerial(got.Serials[0])
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
// require: each rel is its own Link header line (SDKs parse each
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
				// Ably emits the "./"-prefixed resource form (e.g.
				// "./messages?..."); ably-js's parseRelLinks only matches that.
				if !strings.HasPrefix(linkURL, "./"+tc.base) {
					t.Errorf("Link URL %q does not start with resource segment %q", linkURL, "./"+tc.base)
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

// fakeConnResolver is a test ConnectionResolver mapping one known
// connectionKey to a fixed connectionId; every other key is unresolvable,
// standing in for the realtime registry (DESIGN.md §13).
type fakeConnResolver struct {
	key    string
	connID string
}

func (f fakeConnResolver) ResolveConnectionKey(key string) (string, bool) {
	if key != "" && key == f.key {
		return f.connID, true
	}
	return "", false
}

// publishFirst attaches a stream, runs the built publish request, and (on a
// 201) returns the response and the first delivered message; on any other
// status it returns the response and nil, so error cases can assert the code.
func publishFirst(t *testing.T, srv *httptest.Server, manager *core.Manager, channel string, build func() *http.Request) (*http.Response, *protocol.Message) {
	t.Helper()
	stream := attachStream(t, manager, channel)
	resp, err := srv.Client().Do(build())
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusCreated {
		return resp, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(cm.Messages) == 0 {
		t.Fatalf("no messages delivered")
	}
	return resp, cm.Messages[0]
}

// TestPublishStampsImplicitClientID covers the §3.2 / RSL1m1 identity rules
// on the REST publish path: a concrete auth identity (token claim, or a
// bare-key request narrowed by the clientId param) is stamped onto a message
// that omits one, while a wildcard/anonymous identity stamps nothing (and
// never the literal "*").
func TestPublishStampsImplicitClientID(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	authr := auth.NewAuthenticator(parsed)
	token, _, _, _, err := authr.MintToken(&auth.TokenRequest{KeyName: "app.key", ClientID: "tok_client"})
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}

	tests := []struct {
		name    string
		channel string
		path    string
		auth    func(*http.Request)
		want    string
	}{
		{
			name:    "token identity stamped",
			channel: "impl_tok",
			path:    "/channels/impl_tok/messages",
			auth:    func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) },
			want:    "tok_client",
		},
		{
			name:    "bare key with clientId param stamped",
			channel: "impl_param",
			path:    "/channels/impl_param/messages?clientId=param_client",
			auth:    func(r *http.Request) { r.SetBasicAuth("app.key", "secret") },
			want:    "param_client",
		},
		{
			// RSL1m1: ably-js asserts the client-configured clientId via a
			// base64 X-Ably-ClientId header on a basic-auth request rather
			// than in the message body.
			name:    "bare key with X-Ably-ClientId header stamped",
			channel: "impl_hdr",
			path:    "/channels/impl_hdr/messages",
			auth: func(r *http.Request) {
				r.SetBasicAuth("app.key", "secret")
				r.Header.Set("X-Ably-ClientId", base64.StdEncoding.EncodeToString([]byte("hdr_client")))
			},
			want: "hdr_client",
		},
		{
			name:    "wildcard bare key stamps nothing",
			channel: "impl_wild",
			path:    "/channels/impl_wild/messages",
			auth:    func(r *http.Request) { r.SetBasicAuth("app.key", "secret") },
			want:    "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, manager := newTestServer(t)
			body, _ := json.Marshal(&protocol.Message{Name: "event0"})
			_, got := publishFirst(t, srv, manager, tc.channel, func() *http.Request {
				req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+tc.path, bytes.NewReader(body))
				if err != nil {
					t.Fatalf("new request: %v", err)
				}
				req.Header.Set("Content-Type", "application/json")
				tc.auth(req)
				return req
			})
			if got.ClientID != tc.want {
				t.Errorf("stamped clientId = %q, want %q", got.ClientID, tc.want)
			}
		})
	}
}

// TestPublishOnBehalfConnectionKey covers publish-on-behalf (DESIGN.md §13):
// a resolvable connectionKey stamps only the target connection's
// connectionId and is stripped from the stored message; an unresolvable key
// is Ably 40006. clientId is never inherited from the target connection —
// per the reference server, it always comes from the REST request's own
// resolved identity (§3.2), so an anonymous publisher stays anonymous even
// when publishing on behalf of a connection that has a clientId.
func TestPublishOnBehalfConnectionKey(t *testing.T) {
	resolver := fakeConnResolver{key: "conn-key-123", connID: "AbCdEfGhIjKl"}

	t.Run("valid key stamps connectionId only, not the connection's clientId", func(t *testing.T) {
		parsed, err := auth.ParseAPIKey(testKey)
		if err != nil {
			t.Fatalf("parse api key: %v", err)
		}
		srv, manager := newTestServerWithResolver(t, resolver, parsed)
		body, _ := json.Marshal(&protocol.Message{Name: "foo", Data: "bar", ConnectionKey: "conn-key-123"})
		_, got := publishFirst(t, srv, manager, "onbehalf", func() *http.Request {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/channels/onbehalf/messages", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.SetBasicAuth("app.key", "secret")
			return req
		})
		if got.ConnectionID != resolver.connID {
			t.Errorf("connectionId = %q, want %q", got.ConnectionID, resolver.connID)
		}
		if got.ClientID != "" {
			t.Errorf("clientId = %q, want empty (anonymous publisher, not inherited from the resolved connection)", got.ClientID)
		}
		if got.ConnectionKey != "" {
			t.Errorf("stored connectionKey = %q, want it stripped", got.ConnectionKey)
		}
	})

	t.Run("unknown key is 40006", func(t *testing.T) {
		parsed, err := auth.ParseAPIKey(testKey)
		if err != nil {
			t.Fatalf("parse api key: %v", err)
		}
		srv, _ := newTestServerWithResolver(t, resolver, parsed)
		body, _ := json.Marshal(&protocol.Message{Name: "foo", ConnectionKey: "no-such-key"})
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/channels/onbehalf/messages", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth("app.key", "secret")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
		if hdr := resp.Header.Get("X-Ably-Errorcode"); hdr != "40006" {
			t.Errorf("X-Ably-Errorcode = %q, want 40006", hdr)
		}
	})
}

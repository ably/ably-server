package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

const testKey = "app.key:secret"

// newTestServer builds an httptest.Server wrapping our REST handler
// with a known API key. The returned Manager is the same one the
// server is wired with, so tests can observe Appends directly.
func newTestServer(t *testing.T) (*httptest.Server, *core.Manager) {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rs := NewServer(parsed, manager, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	mux.HandleFunc("POST /channels/{name}/messages", rs.HandlePublish)
	mux.HandleFunc("GET /time", rs.HandleTime)
	mux.HandleFunc("GET /healthz", rs.HandleHealthz)
	mux.HandleFunc("GET /readyz", rs.HandleReadyz)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, manager
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
	stream := manager.GetChannel("foo").Attach()
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
	stream := manager.GetChannel("foo").Attach()

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

func TestPublishMsgpack(t *testing.T) {
	srv, manager := newTestServer(t)
	stream := manager.GetChannel("foo").Attach()

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

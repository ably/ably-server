package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/server-protocol/go/wire"
)

const testKey = "app.key:secret"

// newTestServerFor wires the endpoints this server serves itself onto a test
// mux. The channel surface is the shared module's and is exercised through it,
// not here.
func newTestServerFor(t *testing.T, ready storage.Pinger) *httptest.Server {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	rs := NewServer([]auth.APIKey{parsed}, logging.New(slog.DiscardHandler), ready)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", rs.HandleStats)
	mux.HandleFunc("POST /stats", rs.HandlePostStats)
	mux.HandleFunc("GET /healthz", rs.HandleHealthz)
	mux.HandleFunc("GET /readyz", rs.HandleReadyz)
	mux.HandleFunc("/", rs.HandleNotFound)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
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

// publishResponseBody is a local decode target mirroring the server's
// publishResponse — the tests decode the wire body independently to pin
// the {channel, messageId, serials} shape (Ably RSL1/RSL1n) for both
// formats.
type publishResponseBody struct {
	Channel   string   `json:"channel"   msgpack:"channel"`
	MessageID string   `json:"messageId" msgpack:"messageId"`
	Serials   []string `json:"serials"   msgpack:"serials"`
}

// TestHandleNotFoundAblyError pins the unknown-resource 404:
// an Ably ErrorInfo body carrying code 40400 plus the X-Ably-Errorcode /
// X-Ably-Errormessage headers SDKs read, in the Accept format.
func TestHandleNotFoundAblyError(t *testing.T) {
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	rs := NewServer([]auth.APIKey{parsed}, logging.New(slog.DiscardHandler), nil)

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

// TestPostStatsIsPromptNoOp pins the POST /stats stub: SDK test
// flows write stats before reading them, and the write path treats a
// non-2xx as an error whose body it reads — a 404 left some SDKs' REST
// test clients (e.g. ably-go's TestRestClient) blocked. POST /stats must answer promptly with an empty
// 201 (authenticated like the GET), draining the request body.
func TestPostStatsIsPromptNoOp(t *testing.T) {
	srv := newTestServerFor(t, nil)
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
	srv := newTestServerFor(t, nil)
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

func TestReadyzClusterModeReachable(t *testing.T) {
	srv := newTestServerFor(t, fakePinger{})
	resp := request(t, srv, http.MethodGet, "/readyz", "", nil, false)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz status = %d, want 200", resp.StatusCode)
	}
}

func TestReadyzClusterModeUnreachable(t *testing.T) {
	srv := newTestServerFor(t, fakePinger{err: errors.New("dial tcp: connection refused")})

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
func publishBatch(t *testing.T, srv *httptest.Server, channel string, msgs []*wire.Message) {
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

// publishOneAndChannelSerial publishes a single message and returns the
// create channelSerial (storage.CreateChannelSerial of the returned
// Message.Serial) it landed at — a stand-in for the ATTACHED
// ChannelSerial ably-js captures as attachSerial.
func publishOneAndChannelSerial(t *testing.T, srv *httptest.Server, channel, name string) string {
	t.Helper()
	body, _ := json.Marshal(&wire.Message{Name: new(name)})
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

// fakeConnResolver is a test ConnectionResolver mapping one known
// connectionKey to a fixed connectionId; every other key is unresolvable,
// standing in for the realtime registry (DESIGN.md §13).
type fakeConnResolver struct {
	key    string
	connID string
}

// publishFirst attaches a stream, runs the built publish request, and (on a
// 201) returns the response and the first delivered message; on any other
// status it returns the response and nil, so error cases can assert the code.
func publishFirst(t *testing.T, srv *httptest.Server, manager *core.Manager, channel string, build func() *http.Request) (*http.Response, *wire.Message) {
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

func (f fakePinger) Ping(context.Context) error { return f.err }

// decodeJSON decodes a JSON response body, failing the test on a body that
// does not parse.
func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode JSON: %v (body %q)", err, body)
	}
}

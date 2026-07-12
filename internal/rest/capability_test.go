package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/protocol"
)

// bearerToken mints an HS256 JWT carrying the given capability claim,
// signed with the test key's secret and its name as kid.
func bearerToken(t *testing.T, capability string) string {
	return bearerTokenWithClientID(t, capability, "")
}

// bearerTokenWithClientID mints a token with an optional x-ably-clientId
// claim in addition to the capability.
func bearerTokenWithClientID(t *testing.T, capability, clientID string) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	if capability != "" {
		claims["x-ably-capability"] = capability
	}
	if clientID != "" {
		claims["x-ably-clientId"] = clientID
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tok.Header["kid"] = "app.key"
	s, err := tok.SignedString([]byte("secret"))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

// tokenRequest issues a request authenticated with a Bearer token.
func tokenRequest(t *testing.T, srv *httptest.Server, method, path, token string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestRESTCapabilityEnforcement(t *testing.T) {
	srv, _ := newTestServer(t)

	// Capability granting only publish on news:*.
	pub := bearerToken(t, `{"news:*":["publish"]}`)
	msg, _ := json.Marshal(&protocol.Message{Name: "n", Data: "x"})

	// Publish to a permitted channel: 201.
	if resp := tokenRequest(t, srv, http.MethodPost, "/channels/news:sport/messages", pub, msg); resp.StatusCode != http.StatusCreated {
		t.Errorf("publish news:sport status = %d, want 201", resp.StatusCode)
	}
	// Publish to a non-permitted channel: 401 with code 40160.
	resp := tokenRequest(t, srv, http.MethodPost, "/channels/other/messages", pub, msg)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("publish other status = %d, want 401", resp.StatusCode)
	}
	assertCapabilityError(t, resp)
	// History requires the history op, which this token lacks: 401.
	if resp := tokenRequest(t, srv, http.MethodGet, "/channels/news:sport/messages", pub, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("history status = %d, want 401", resp.StatusCode)
	}

	// Capability granting history everywhere but not publish.
	hist := bearerToken(t, `{"*":["history"]}`)
	if resp := tokenRequest(t, srv, http.MethodGet, "/channels/news:sport/messages", hist, nil); resp.StatusCode != http.StatusOK {
		t.Errorf("history with history cap status = %d, want 200", resp.StatusCode)
	}
	if resp := tokenRequest(t, srv, http.MethodPost, "/channels/news:sport/messages", hist, msg); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("publish with history-only cap status = %d, want 401", resp.StatusCode)
	}

	// Presence GET requires subscribe.
	sub := bearerToken(t, `{"room:*":["subscribe"]}`)
	if resp := tokenRequest(t, srv, http.MethodGet, "/channels/room:1/presence", sub, nil); resp.StatusCode != http.StatusOK {
		t.Errorf("presence with subscribe cap status = %d, want 200", resp.StatusCode)
	}
	if resp := tokenRequest(t, srv, http.MethodGet, "/channels/room:1/presence", hist, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("presence with history-only cap status = %d, want 401", resp.StatusCode)
	}
}

// publishAs creates a message via a token and returns its stable serial
// and stamped creator clientId (read back from history via Basic auth).
func publishAs(t *testing.T, srv *httptest.Server, channel, token string, m *protocol.Message) (serial, creator string) {
	t.Helper()
	body, _ := json.Marshal(m)
	if resp := tokenRequest(t, srv, http.MethodPost, "/channels/"+channel+"/messages", token, body); resp.StatusCode != http.StatusCreated {
		t.Fatalf("publishAs: status %d, want 201", resp.StatusCode)
	}
	hr := historyGet(t, srv, channel, "direction=forwards", "")
	var msgs []*protocol.Message
	decodeJSON(t, hr, &msgs)
	if len(msgs) == 0 {
		t.Fatal("publishAs: history empty after publish")
	}
	last := msgs[len(msgs)-1]
	return last.Serial, last.ClientID
}

func TestRESTMutationOwnership(t *testing.T) {
	srv, _ := newTestServer(t)

	// alice creates a message on doc:1; she is its creator.
	alice := bearerTokenWithClientID(t, `{"doc:*":["publish","message-update-own","message-delete-own"]}`, "alice")
	serial, creator := publishAs(t, srv, "doc:1", alice, &protocol.Message{Name: "n", Data: "orig"})
	if creator != "alice" {
		t.Fatalf("creator clientId = %q, want alice", creator)
	}
	path := "/channels/doc:1/messages/" + serial
	update, _ := json.Marshal(&protocol.Message{Action: protocol.MessageUpdate, Data: "edited"})

	// own-allowed: alice may update her own message.
	if resp := tokenRequest(t, srv, http.MethodPatch, path, alice, update); resp.StatusCode != http.StatusOK {
		t.Errorf("alice update (own-allowed) status = %d, want 200", resp.StatusCode)
	}

	// own-denied: bob has message-update-own but is not the creator.
	bob := bearerTokenWithClientID(t, `{"doc:*":["message-update-own"]}`, "bob")
	resp := tokenRequest(t, srv, http.MethodPatch, path, bob, update)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("bob update (own-denied) status = %d, want 401", resp.StatusCode)
	}
	assertCapabilityError(t, resp)

	// any-allowed: carol has message-update-any and may update anyone's.
	carol := bearerTokenWithClientID(t, `{"doc:*":["message-update-any"]}`, "carol")
	if resp := tokenRequest(t, srv, http.MethodPatch, path, carol, update); resp.StatusCode != http.StatusOK {
		t.Errorf("carol update (any-allowed) status = %d, want 200", resp.StatusCode)
	}

	// missing-capability: dave has only publish, no mutation op.
	dave := bearerTokenWithClientID(t, `{"doc:*":["publish"]}`, "dave")
	resp = tokenRequest(t, srv, http.MethodPatch, path, dave, update)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("dave update (missing-cap) status = %d, want 401", resp.StatusCode)
	}
	assertCapabilityError(t, resp)

	// delete authorises against message-delete-*: alice has delete-own.
	del, _ := json.Marshal(&protocol.Message{Action: protocol.MessageDelete})
	if resp := tokenRequest(t, srv, http.MethodPatch, path, alice, del); resp.StatusCode != http.StatusOK {
		t.Errorf("alice delete (own-allowed) status = %d, want 200", resp.StatusCode)
	}
	// carol has update-any but NOT delete-any, so cannot delete.
	if resp := tokenRequest(t, srv, http.MethodPatch, path, carol, del); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("carol delete (no delete cap) status = %d, want 401", resp.StatusCode)
	}
}

// TestRESTRestrictedBasicKey exercises a restricted key end to end over
// Basic auth (TASK-93 AC #2): the Basic principal resolves to the key's
// own capability, so an op outside it is denied 40160 while one inside
// succeeds.
func TestRESTRestrictedBasicKey(t *testing.T) {
	key, err := auth.ParseAPIKeyWithCapability("app.sub:s3cr3t", `{"chat:*":["subscribe"]}`)
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	srv, _ := newTestServerWithKeys(t, key)

	basic := func(method, path string, body []byte) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.SetBasicAuth("app.sub", "s3cr3t")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	// Presence GET on chat:* needs subscribe, which the key grants: 200.
	if resp := basic(http.MethodGet, "/channels/chat:room/presence", nil); resp.StatusCode != http.StatusOK {
		t.Errorf("presence chat:room status = %d, want 200", resp.StatusCode)
	}
	// Publish needs publish, which the key lacks: 401/40160.
	msg, _ := json.Marshal(&protocol.Message{Name: "n", Data: "x"})
	resp := basic(http.MethodPost, "/channels/chat:room/messages", msg)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("publish status = %d, want 401", resp.StatusCode)
	}
	assertCapabilityError(t, resp)
	// Subscribe op outside the key's chat:* scope is also denied: 401/40160.
	resp = basic(http.MethodGet, "/channels/other/presence", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("presence other status = %d, want 401", resp.StatusCode)
	}
	assertCapabilityError(t, resp)
}

func assertCapabilityError(t *testing.T, resp *http.Response) {
	t.Helper()
	var body errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error == nil || body.Error.Code != 40160 {
		t.Errorf("error body = %+v, want code 40160", body.Error)
	}
}

// TestStatsStub covers the /stats compatibility stub (DESIGN.md §1):
// authenticated + app-wide `stats` op → an empty array; a narrowed
// token without the op → 401/40160; no credentials → 401.
func TestStatsStub(t *testing.T) {
	srv, _ := newTestServer(t)

	// No credentials: 401.
	resp, err := srv.Client().Get(srv.URL + "/stats")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("unauthenticated status = %d, want 401", resp.StatusCode)
	}

	// Basic auth (full key capability): 200 with an empty JSON array.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("app.key", "secret")
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("basic status = %d, want 200", resp.StatusCode)
	}
	var got []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("stats body has %d elements, want empty array", len(got))
	}

	// Token granting stats app-wide: 200.
	stats := bearerToken(t, `{"*":["stats"]}`)
	if resp := tokenRequest(t, srv, http.MethodGet, "/stats", stats, nil); resp.StatusCode != http.StatusOK {
		t.Errorf("stats-cap status = %d, want 200", resp.StatusCode)
	}
	// Channel-scoped stats grant is not app-wide: 401/40160.
	narrow := bearerToken(t, `{"news:*":["stats"]}`)
	resp = tokenRequest(t, srv, http.MethodGet, "/stats", narrow, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("narrow-cap status = %d, want 401", resp.StatusCode)
	}
	assertCapabilityError(t, resp)

	// msgpack Accept: an empty msgpack array.
	req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("app.key", "secret")
	req.Header.Set("Accept", "application/x-msgpack")
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-msgpack" {
		t.Errorf("msgpack Content-Type = %q", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var arr []any
	if err := msgpack.Unmarshal(raw, &arr); err != nil {
		t.Fatalf("msgpack decode: %v", err)
	}
	if len(arr) != 0 {
		t.Errorf("msgpack stats body has %d elements, want empty array", len(arr))
	}
}

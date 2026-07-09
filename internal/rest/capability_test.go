package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/ably/ably-server/internal/protocol"
)

// bearerToken mints an HS256 JWT carrying the given capability claim,
// signed with the test key's secret and its name as kid.
func bearerToken(t *testing.T, capability string) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	if capability != "" {
		claims["x-ably-capability"] = capability
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

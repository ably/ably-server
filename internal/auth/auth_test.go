package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestParseAPIKey(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantApp string
		wantKey string
		wantSec string
		wantErr bool
	}{
		{name: "valid", in: "app.key:secret", wantApp: "app", wantKey: "key", wantSec: "secret"},
		{name: "secret may contain colons", in: "app.key:sec:ret", wantApp: "app", wantKey: "key", wantSec: "sec:ret"},
		{name: "missing colon", in: "app.keysecret", wantErr: true},
		{name: "missing dot", in: "appkey:secret", wantErr: true},
		{name: "empty appId", in: ".key:secret", wantErr: true},
		{name: "empty keyId", in: "app.:secret", wantErr: true},
		{name: "empty secret", in: "app.key:", wantErr: true},
		{name: "empty", in: "", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseAPIKey(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseAPIKey(%q) = %+v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseAPIKey(%q): %v", tc.in, err)
			}
			if got.AppID != tc.wantApp || got.KeyID != tc.wantKey || got.KeySecret != tc.wantSec {
				t.Errorf("ParseAPIKey(%q) = (%q, %q, %q), want (%q, %q, %q)",
					tc.in, got.AppID, got.KeyID, got.KeySecret, tc.wantApp, tc.wantKey, tc.wantSec)
			}
		})
	}
}

func TestAuthenticate(t *testing.T) {
	const validKey = "app.key:secret"
	parsed, err := ParseAPIKey(validKey)
	if err != nil {
		t.Fatalf("setup: ParseAPIKey: %v", err)
	}
	a := NewAuthenticator(parsed)

	makeReq := func(setup func(*http.Request)) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if setup != nil {
			setup(r)
		}
		return r
	}

	tests := []struct {
		name    string
		req     *http.Request
		wantErr error
	}{
		{
			name: "basic auth header",
			req: makeReq(func(r *http.Request) {
				r.SetBasicAuth("app.key", "secret")
			}),
		},
		{
			name: "query parameter",
			req:  httptest.NewRequest(http.MethodGet, "/?key=app.key:secret", nil),
		},
		{
			name:    "no credentials",
			req:     makeReq(nil),
			wantErr: ErrNoCredentials,
		},
		{
			name:    "wrong basic auth",
			req:     makeReq(func(r *http.Request) { r.SetBasicAuth("app.key", "wrong") }),
			wantErr: ErrInvalidKey,
		},
		{
			name:    "wrong query parameter",
			req:     httptest.NewRequest(http.MethodGet, "/?key=app.key:wrong", nil),
			wantErr: ErrInvalidKey,
		},
		{
			name: "basic header beats query param when both present",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/?key=app.key:wrong", nil)
				r.SetBasicAuth("app.key", "secret")
				return r
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := a.Authenticate(tc.req)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Authenticate: %v, want success", err)
				}
				if got == nil || got.Method != MethodBasic {
					t.Fatalf("Authenticate principal = %+v, want Basic", got)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Authenticate err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

const (
	testKey    = "app.key:secret"
	testSecret = "secret"
)

// mintToken builds an HS256 JWT signed with secret carrying claims.
func mintToken(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tok.Header["kid"] = "app.key"
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("mintToken: %v", err)
	}
	return s
}

// TestParseAPIKeyWithCapability covers the per-key capability parsing:
// a bare key grants the full capability, an explicit
// capability is parsed and attached, and a malformed one errors.
func TestParseAPIKeyWithCapability(t *testing.T) {
	full, err := ParseAPIKey("app.key:secret")
	if err != nil {
		t.Fatalf("ParseAPIKey: %v", err)
	}
	if !full.Capability().Permits("anything", OpPublish) {
		t.Errorf("bare key should grant full capability")
	}

	scoped, err := ParseAPIKeyWithCapability("app.key:secret", `{"chat:*":["subscribe"]}`)
	if err != nil {
		t.Fatalf("ParseAPIKeyWithCapability: %v", err)
	}
	if !scoped.Capability().Permits("chat:room", OpSubscribe) {
		t.Errorf("scoped key should grant subscribe on chat:room")
	}
	if scoped.Capability().Permits("chat:room", OpPublish) {
		t.Errorf("scoped key should not grant publish")
	}
	if scoped.Capability().Permits("other", OpSubscribe) {
		t.Errorf("scoped key should not grant subscribe outside chat:*")
	}

	if _, err := ParseAPIKeyWithCapability("app.key:secret", `not json`); err == nil {
		t.Errorf("malformed capability should error")
	}
}

// TestAuthenticateBasicResolvesKeyCapability verifies a Basic-auth
// principal resolves to the authenticating key's capability, not the
// permissive all-access set.
func TestAuthenticateBasicResolvesKeyCapability(t *testing.T) {
	restricted, err := ParseAPIKeyWithCapability("app.sub:secret", `{"chat:*":["subscribe"]}`)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	a := NewAuthenticator(restricted)

	r := httptest.NewRequest(http.MethodGet, "/?key=app.sub:secret", nil)
	p, err := a.Authenticate(r)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !p.Capabilities().Permits("chat:room", OpSubscribe) {
		t.Errorf("Basic principal should inherit the key's subscribe grant")
	}
	if p.Capabilities().Permits("chat:room", OpPublish) {
		t.Errorf("Basic principal should not gain publish beyond the key")
	}
}

// TestAuthenticateJWTIntersectsKeyCapability verifies a token's effective
// capability is the claim intersected with the signing key's capability,
// and that an absent claim inherits the key's capability.
func TestAuthenticateJWTIntersectsKeyCapability(t *testing.T) {
	key, err := ParseAPIKeyWithCapability("app.key:secret", `{"chat:*":["publish","subscribe"]}`)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	a := NewAuthenticator(key)
	now := time.Now()

	t.Run("absent claim inherits key capability", func(t *testing.T) {
		tok := mintToken(t, "secret", jwt.MapClaims{"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		p, err := a.Authenticate(r)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if !p.Capabilities().Permits("chat:room", OpPublish) {
			t.Errorf("absent claim should inherit key's publish grant")
		}
		if p.Capabilities().Permits("other", OpPublish) {
			t.Errorf("absent claim must not exceed the key's scope")
		}
	})

	t.Run("claim intersected with key capability", func(t *testing.T) {
		// Claim asks for publish+subscribe on all channels; key only
		// grants chat:* — the intersection is publish/subscribe on chat:*.
		tok := mintToken(t, "secret", jwt.MapClaims{
			"iat":               now.Unix(),
			"exp":               now.Add(time.Hour).Unix(),
			"x-ably-capability": `{"*":["publish","subscribe"]}`,
		})
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		p, err := a.Authenticate(r)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if !p.Capabilities().Permits("chat:room", OpSubscribe) {
			t.Errorf("intersection should grant subscribe on chat:room")
		}
		if p.Capabilities().Permits("other", OpSubscribe) {
			t.Errorf("intersection must not grant beyond the key's chat:* scope")
		}
	})

	t.Run("claim narrower than key", func(t *testing.T) {
		tok := mintToken(t, "secret", jwt.MapClaims{
			"iat":               now.Unix(),
			"exp":               now.Add(time.Hour).Unix(),
			"x-ably-capability": `{"chat:*":["subscribe"]}`,
		})
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		p, err := a.Authenticate(r)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if p.Capabilities().Permits("chat:room", OpPublish) {
			t.Errorf("claim narrowing away publish should deny it")
		}
		if !p.Capabilities().Permits("chat:room", OpSubscribe) {
			t.Errorf("claim should retain subscribe")
		}
	})
}

// TestMintTokenNarrowsAgainstKeyCapability verifies requestToken minting
// narrows the requested capability against the signing key's own
// capability, rejecting a request the key cannot grant.
func TestMintTokenNarrowsAgainstKeyCapability(t *testing.T) {
	key, err := ParseAPIKeyWithCapability("app.key:secret", `{"chat:*":["subscribe"]}`)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	a := NewAuthenticator(key)

	// Requesting publish (which the key does not grant) is rejected.
	if _, _, _, _, err := a.MintToken(&TokenRequest{
		KeyName:    "app.key",
		Capability: `{"chat:*":["publish"]}`,
	}); !errors.Is(err, ErrCapabilityDenied) {
		t.Errorf("minting a capability the key cannot grant: err = %v, want ErrCapabilityDenied", err)
	}

	// Requesting subscribe on all channels is clamped to the key's chat:*.
	tok, _, _, _, err := a.MintToken(&TokenRequest{
		KeyName:    "app.key",
		Capability: `{"*":["subscribe"]}`,
	})
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}
	p, err := a.VerifyToken(tok)
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if !p.Capabilities().Permits("chat:room", OpSubscribe) {
		t.Errorf("minted token should grant subscribe on chat:room")
	}
	if p.Capabilities().Permits("other", OpSubscribe) {
		t.Errorf("minted token must be clamped to the key's chat:* scope")
	}
}

// TestMintTokenRejectsExcessiveTTL checks MintToken enforces maxTokenTTL
// itself (DESIGN.md §3.3), rather than relying solely on a caller having
// already run ValidateTTL — defense-in-depth against a caller that skips
// or mis-orders that check.
func TestMintTokenRejectsExcessiveTTL(t *testing.T) {
	parsed, _ := ParseAPIKey(testKey)
	a := NewAuthenticator(parsed)

	if _, _, _, _, err := a.MintToken(&TokenRequest{
		KeyName: "app.key",
		TTL:     (25 * time.Hour).Milliseconds(), // exceeds the 24h maximum
	}); !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("excessive ttl: err = %v, want ErrInvalidTTL", err)
	}

	if _, _, _, _, err := a.MintToken(&TokenRequest{
		KeyName: "app.key",
		TTL:     (24 * time.Hour).Milliseconds(), // exactly at the maximum
	}); err != nil {
		t.Errorf("ttl at maximum: err = %v, want nil", err)
	}
}

func TestAuthenticateJWT(t *testing.T) {
	parsed, err := ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("setup: ParseAPIKey: %v", err)
	}
	a := NewAuthenticator(parsed)

	now := time.Now()
	valid := jwt.MapClaims{"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}

	t.Run("valid token via Authorization: Bearer", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", "Bearer "+mintToken(t, testSecret, valid))
		p, err := a.Authenticate(r)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if p.Method != MethodToken {
			t.Errorf("Method = %v, want Token", p.Method)
		}
	})

	t.Run("valid token via Authorization: Bearer base64 (RSA3a)", func(t *testing.T) {
		// Ably SDKs base64-encode the JWT in the Authorization header
		// (RSA3a); it must verify identically to the raw form. This is the
		// transport the JWT-as-token flow (TestAuth_JWT_Token_RSA8c) uses,
		// with a minimal iat/exp token like ably's echo server mints.
		enc := base64.StdEncoding.EncodeToString([]byte(mintToken(t, testSecret, valid)))
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", "Bearer "+enc)
		p, err := a.Authenticate(r)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if p.Method != MethodToken {
			t.Errorf("Method = %v, want Token", p.Method)
		}
	})

	t.Run("valid token via access_token and accessToken params", func(t *testing.T) {
		for _, param := range []string{"access_token", "accessToken"} {
			tok := mintToken(t, testSecret, valid)
			r := httptest.NewRequest(http.MethodGet, "/?"+param+"="+tok, nil)
			if _, err := a.Authenticate(r); err != nil {
				t.Errorf("%s: Authenticate: %v", param, err)
			}
		}
	})

	t.Run("claims exposed for downstream resolution", func(t *testing.T) {
		tok := mintToken(t, testSecret, jwt.MapClaims{
			"iat":               now.Unix(),
			"exp":               now.Add(time.Hour).Unix(),
			"x-ably-capability": `{"chat:*":["publish"]}`,
			"x-ably-clientId":   "alice",
		})
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		p, err := a.Authenticate(r)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if p.Capability != `{"chat:*":["publish"]}` {
			t.Errorf("Capability = %q", p.Capability)
		}
		if !p.HasClientID || p.ClientID != "alice" {
			t.Errorf("ClientID = %q (has=%v), want alice", p.ClientID, p.HasClientID)
		}
	})

	t.Run("wildcard clientId preserved", func(t *testing.T) {
		tok := mintToken(t, testSecret, jwt.MapClaims{
			"iat":             now.Unix(),
			"exp":             now.Add(time.Hour).Unix(),
			"x-ably-clientId": "*",
		})
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		p, err := a.Authenticate(r)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if !p.HasClientID || p.ClientID != "*" {
			t.Errorf("ClientID = %q (has=%v), want *", p.ClientID, p.HasClientID)
		}
	})

	t.Run("forward iat clock skew accepted", func(t *testing.T) {
		// A small forward clock skew on iat is tolerated; exp is not given
		// any grace, so the token must still be unexpired.
		tok := mintToken(t, testSecret, jwt.MapClaims{
			"iat": now.Add(30 * time.Second).Unix(), // slightly future, within leeway
			"exp": now.Add(time.Hour).Unix(),
		})
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		if _, err := a.Authenticate(r); err != nil {
			t.Fatalf("Authenticate: %v, want success within iat leeway", err)
		}
	})

	t.Run("expired token rejected with no grace", func(t *testing.T) {
		// A token expired by a second is rejected outright (no leeway on exp),
		// and surfaced as the renewable token-expired error (DESIGN.md §3).
		tok := mintToken(t, testSecret, jwt.MapClaims{
			"iat": now.Add(-time.Minute).Unix(),
			"exp": now.Add(-time.Second).Unix(),
		})
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		if _, err := a.Authenticate(r); !errors.Is(err, ErrTokenExpired) {
			t.Fatalf("Authenticate err = %v, want ErrTokenExpired", err)
		}
	})

	t.Run("malformed JWT gets its own error code", func(t *testing.T) {
		// Not a well-formed JWT at all (wrong segment count), as distinct from
		// a claims-level rejection — Ably surfaces this as 40144 rather than
		// the generic 40101 invalid-credentials.
		r := httptest.NewRequest(http.MethodGet, "/?access_token=not-a-jwt", nil)
		_, err := a.Authenticate(r)
		if !errors.Is(err, ErrInvalidJWT) {
			t.Fatalf("Authenticate err = %v, want ErrInvalidJWT", err)
		}
		if code, status, _ := AuthErrorInfo(err); code != 40144 || status != 401 {
			t.Errorf("AuthErrorInfo = (%d, %d), want (40144, 401)", code, status)
		}
	})

	t.Run("bad signature gets the JWT error code, not the generic one", func(t *testing.T) {
		// The reference server verifies the signature as part of decoding the
		// JWT (auth.go validateJWS' token.Claims call), so a signature failure
		// is bucketed with other invalid-JWT cases (40144), not the generic
		// invalid-credentials 40101.
		tok := mintToken(t, "wrong-secret", valid)
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		_, err := a.Authenticate(r)
		if !errors.Is(err, ErrInvalidJWT) {
			t.Fatalf("Authenticate err = %v, want ErrInvalidJWT", err)
		}
		if code, _, _ := AuthErrorInfo(err); code != 40144 {
			t.Errorf("AuthErrorInfo code = %d, want 40144", code)
		}
	})

	rejections := []struct {
		name   string
		claims jwt.MapClaims
		secret string
	}{
		{name: "expired beyond leeway", claims: jwt.MapClaims{"iat": now.Add(-2 * time.Hour).Unix(), "exp": now.Add(-time.Hour).Unix()}, secret: testSecret},
		{name: "iat in future beyond leeway", claims: jwt.MapClaims{"iat": now.Add(time.Hour).Unix(), "exp": now.Add(2 * time.Hour).Unix()}, secret: testSecret},
		{name: "missing iat", claims: jwt.MapClaims{"exp": now.Add(time.Hour).Unix()}, secret: testSecret},
		{name: "missing exp", claims: jwt.MapClaims{"iat": now.Unix()}, secret: testSecret},
	}
	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			tok := mintToken(t, tc.secret, tc.claims)
			r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
			_, err := a.Authenticate(r)
			if !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("Authenticate err = %v, want ErrInvalidToken", err)
			}
		})
	}

	t.Run("non-HS256 algorithm rejected", func(t *testing.T) {
		// "none" alg: an unsigned token must not be accepted. Rejected as an
		// invalid signing method, so it's an invalid-JWT case (40144).
		tok := jwt.NewWithClaims(jwt.SigningMethodNone, valid)
		s, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
		if err != nil {
			t.Fatalf("sign none: %v", err)
		}
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+s, nil)
		if _, err := a.Authenticate(r); !errors.Is(err, ErrInvalidJWT) {
			t.Fatalf("Authenticate err = %v, want ErrInvalidJWT", err)
		}
	})
}

func TestAuthenticateMultipleKeys(t *testing.T) {
	k1, _ := ParseAPIKey("app.key1:secret1")
	k2, _ := ParseAPIKey("app.key2:secret2")
	a := NewAuthenticator(k1, k2)

	// Either key authenticates via Basic / ?key=.
	for _, spec := range []string{"app.key1:secret1", "app.key2:secret2"} {
		r := httptest.NewRequest(http.MethodGet, "/?key="+spec, nil)
		if _, err := a.Authenticate(r); err != nil {
			t.Errorf("Authenticate(key=%s): %v", spec, err)
		}
	}

	// A key that is not configured is rejected.
	r := httptest.NewRequest(http.MethodGet, "/?key=app.key3:secret3", nil)
	if _, err := a.Authenticate(r); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("unconfigured key err = %v, want ErrInvalidKey", err)
	}

	now := time.Now()
	claims := jwt.MapClaims{"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}

	// A JWT is verified against the key its kid header names.
	t.Run("kid selects the signing key", func(t *testing.T) {
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tok.Header["kid"] = "app.key2"
		s, err := tok.SignedString([]byte("secret2"))
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+s, nil)
		if _, err := a.Authenticate(r); err != nil {
			t.Errorf("Authenticate kid=app.key2 token: %v", err)
		}
	})

	// A kid naming key2 but signed with key1's secret must NOT verify — a
	// signature failure, so it's the invalid-JWT case (40144), matching the
	// reference server's validateJWS (which verifies the signature as part
	// of decoding).
	t.Run("kid mismatch fails", func(t *testing.T) {
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tok.Header["kid"] = "app.key2"
		s, _ := tok.SignedString([]byte("secret1"))
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+s, nil)
		if _, err := a.Authenticate(r); !errors.Is(err, ErrInvalidJWT) {
			t.Errorf("err = %v, want ErrInvalidJWT", err)
		}
	})

	// A token with no kid header at all is rejected, even if its signature
	// would verify against a configured key's secret — the reference server
	// (auth.go validateJWS) explicitly buckets "kid not found in header"
	// under the invalid-JWT code (40144).
	t.Run("no kid is rejected", func(t *testing.T) {
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		s, _ := tok.SignedString([]byte("secret2"))
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+s, nil)
		if _, err := a.Authenticate(r); !errors.Is(err, ErrInvalidJWT) {
			t.Errorf("no-kid token err = %v, want ErrInvalidJWT", err)
		}
	})

	// A kid naming an unknown KEY on THIS app (same appId, different keyId)
	// is the right-app-wrong-key case: a renewable invalid-JWT (40144), so
	// an SDK retries auth. Distinct from the unknown-APP case below.
	t.Run("unknown key on this app is invalid-JWT", func(t *testing.T) {
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tok.Header["kid"] = "app.key3"
		s, _ := tok.SignedString([]byte("secret2"))
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+s, nil)
		_, err := a.Authenticate(r)
		if !errors.Is(err, ErrInvalidJWT) {
			t.Errorf("unknown-key token err = %v, want ErrInvalidJWT", err)
		}
		if errors.Is(err, ErrUnknownApp) {
			t.Errorf("unknown-key token wrongly classified as ErrUnknownApp: %v", err)
		}
		if code, status, _ := AuthErrorInfo(err); code != 40144 || status != 401 {
			t.Errorf("AuthErrorInfo = (%d, %d), want (40144, 401)", code, status)
		}
	})

	// A kid whose appId is not the one this server hosts is a non-renewable
	// unknown-app failure: 40400/404, deliberately outside the 40140-40149
	// range SDKs treat as renewable, so the SDK surfaces the real error
	// rather than looping on token renewal.
	t.Run("unknown app is 40400 not renewable", func(t *testing.T) {
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tok.Header["kid"] = "otherapp.somekey"
		s, _ := tok.SignedString([]byte("secret2"))
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+s, nil)
		_, err := a.Authenticate(r)
		if !errors.Is(err, ErrUnknownApp) {
			t.Errorf("unknown-app token err = %v, want ErrUnknownApp", err)
		}
		code, status, _ := AuthErrorInfo(err)
		if code != 40400 || status != 404 {
			t.Errorf("AuthErrorInfo = (%d, %d), want (40400, 404)", code, status)
		}
		if code >= 40140 && code < 40150 {
			t.Errorf("unknown-app code %d falls in the renewable 40140-40149 range", code)
		}
	})
}

func TestResolveClientID(t *testing.T) {
	basic := &Principal{Method: MethodBasic}
	tokenNoClaim := &Principal{Method: MethodToken}
	tokenConcrete := &Principal{Method: MethodToken, ClientID: "bob", HasClientID: true}
	tokenWildcard := &Principal{Method: MethodToken, ClientID: WildcardClientID, HasClientID: true}

	cases := []struct {
		name    string
		p       *Principal
		param   string
		want    string
		wantErr bool
	}{
		{name: "basic no param is wildcard", p: basic, param: "", want: WildcardClientID},
		{name: "basic param pins identity", p: basic, param: "alice", want: "alice"},
		{name: "token no claim, no param is anonymous", p: tokenNoClaim, param: "", want: ""},
		{name: "token no claim, param rejected", p: tokenNoClaim, param: "alice", wantErr: true},
		{name: "token concrete claim, no param", p: tokenConcrete, param: "", want: "bob"},
		{name: "token concrete claim, matching param", p: tokenConcrete, param: "bob", want: "bob"},
		{name: "token concrete claim, mismatched param rejected", p: tokenConcrete, param: "alice", wantErr: true},
		{name: "token wildcard, no param retains wildcard", p: tokenWildcard, param: "", want: WildcardClientID},
		{name: "token wildcard, param narrows", p: tokenWildcard, param: "alice", want: "alice"},
		{name: "token wildcard, literal star param rejected", p: tokenWildcard, param: WildcardClientID, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveClientID(tc.p, tc.param)
			if tc.wantErr {
				if !errors.Is(err, ErrClientIDMismatch) {
					t.Fatalf("err = %v, want ErrClientIDMismatch", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Errorf("ResolveClientID = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMessageClientID(t *testing.T) {
	cases := []struct {
		name      string
		conn, msg string
		wantStamp string
		wantOK    bool
	}{
		{name: "anonymous, no msg id", conn: "", msg: "", wantStamp: "", wantOK: true},
		{name: "anonymous asserting id rejected", conn: "", msg: "x", wantOK: false},
		{name: "wildcard, no msg id stays unidentified", conn: WildcardClientID, msg: "", wantStamp: "", wantOK: true},
		{name: "wildcard assumes any id", conn: WildcardClientID, msg: "x", wantStamp: "x", wantOK: true},
		{name: "wildcard literal star rejected", conn: WildcardClientID, msg: WildcardClientID, wantOK: false},
		{name: "concrete, no msg id stamps conn", conn: "alice", msg: "", wantStamp: "alice", wantOK: true},
		{name: "concrete, matching id", conn: "alice", msg: "alice", wantStamp: "alice", wantOK: true},
		{name: "concrete, mismatched id rejected", conn: "alice", msg: "bob", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stamp, ok := MessageClientID(tc.conn, tc.msg)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && stamp != tc.wantStamp {
				t.Errorf("stamp = %q, want %q", stamp, tc.wantStamp)
			}
		})
	}
}

// signTokenRequestForTest independently reproduces the RSA9 mac so the
// tests guard the canonical text format, not just call the code under test.
func signTokenRequestForTest(tr *TokenRequest, secret string) string {
	ttl := ""
	if tr.TTL != 0 {
		ttl = strconv.FormatInt(tr.TTL, 10)
	}
	text := tr.KeyName + "\n" + ttl + "\n" + tr.Capability + "\n" + tr.ClientID + "\n" +
		strconv.FormatInt(tr.Timestamp, 10) + "\n" + tr.Nonce + "\n"
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(text))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func TestValidateTokenRequest(t *testing.T) {
	parsed, _ := ParseAPIKey(testKey)
	a := NewAuthenticator(parsed)
	// A fresh timestamp per request keeps it inside the recency window; a
	// distinct nonce avoids the replay guard tripping across subtests.
	var nonceSeq int
	base := func() *TokenRequest {
		nonceSeq++
		return &TokenRequest{KeyName: "app.key", TTL: 3600000, Capability: `{"*":["*"]}`, Timestamp: time.Now().UnixMilli(), Nonce: fmt.Sprintf("nonce-%d", nonceSeq)}
	}

	t.Run("valid mac accepted", func(t *testing.T) {
		tr := base()
		tr.MAC = signTokenRequestForTest(tr, testSecret)
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); err != nil {
			t.Fatalf("ValidateTokenRequest: %v", err)
		}
	})

	t.Run("tampered mac rejected", func(t *testing.T) {
		tr := base()
		tr.MAC = signTokenRequestForTest(tr, testSecret)
		tr.Capability = `{"*":["subscribe"]}` // change a signed field after signing
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("err = %v, want ErrInvalidToken", err)
		}
	})

	t.Run("wrong key rejected", func(t *testing.T) {
		tr := base()
		tr.KeyName = "other.key"
		tr.MAC = signTokenRequestForTest(tr, testSecret)
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("err = %v, want ErrInvalidToken", err)
		}
	})

	t.Run("unsigned accepted with matching basic auth", func(t *testing.T) {
		tr := base() // no mac
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.SetBasicAuth("app.key", "secret")
		if err := a.ValidateTokenRequest(tr, r); err != nil {
			t.Fatalf("ValidateTokenRequest: %v", err)
		}
	})

	t.Run("unsigned rejected without basic auth", func(t *testing.T) {
		tr := base() // no mac, no basic
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("err = %v, want ErrInvalidToken", err)
		}
	})

	t.Run("stale timestamp rejected", func(t *testing.T) {
		tr := base()
		tr.Timestamp = time.Now().Add(-30 * time.Minute).UnixMilli()
		tr.MAC = signTokenRequestForTest(tr, testSecret)
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); !errors.Is(err, ErrTimestampNotCurrent) {
			t.Fatalf("err = %v, want ErrTimestampNotCurrent", err)
		}
	})

	t.Run("replayed nonce rejected", func(t *testing.T) {
		tr := base()
		tr.MAC = signTokenRequestForTest(tr, testSecret)
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); err != nil {
			t.Fatalf("first use: %v", err)
		}
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); !errors.Is(err, ErrNonceReplayed) {
			t.Fatalf("err = %v, want ErrNonceReplayed", err)
		}
	})

	t.Run("mac-authenticated without timestamp rejected", func(t *testing.T) {
		tr := base()
		tr.Timestamp = 0
		tr.MAC = signTokenRequestForTest(tr, testSecret)
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); !errors.Is(err, ErrTimestampNotCurrent) {
			t.Fatalf("err = %v, want ErrTimestampNotCurrent", err)
		}
	})

	t.Run("mac-authenticated without nonce rejected", func(t *testing.T) {
		tr := base()
		tr.Nonce = ""
		tr.MAC = signTokenRequestForTest(tr, testSecret)
		if err := a.ValidateTokenRequest(tr, httptest.NewRequest(http.MethodPost, "/", nil)); !errors.Is(err, ErrNonceReplayed) {
			t.Fatalf("err = %v, want ErrNonceReplayed", err)
		}
	})

	t.Run("basic auth without timestamp or nonce still accepted", func(t *testing.T) {
		tr := base() // no mac
		tr.Timestamp = 0
		tr.Nonce = ""
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.SetBasicAuth("app.key", "secret")
		if err := a.ValidateTokenRequest(tr, r); err != nil {
			t.Fatalf("ValidateTokenRequest: %v", err)
		}
	})
}

func TestMintTokenRoundTrip(t *testing.T) {
	parsed, _ := ParseAPIKey(testKey)
	a := NewAuthenticator(parsed)

	tr := &TokenRequest{KeyName: "app.key", TTL: 3600000, Capability: `{"*":["*"]}`, ClientID: "alice", Nonce: "n1"}
	tok, _, _, _, err := a.MintToken(tr)
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}

	// The minted token must verify via the normal path, presented the way
	// SDKs send it: Authorization: Bearer base64(token).
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer "+base64.StdEncoding.EncodeToString([]byte(tok)))
	p, err := a.Authenticate(r)
	if err != nil {
		t.Fatalf("Authenticate minted token: %v", err)
	}
	if p.Method != MethodToken || p.ClientID != "alice" || p.Capability != `{"*":["*"]}` {
		t.Errorf("principal = %+v, want token/alice/full-capability", p)
	}

	// Distinct nonces yield distinct tokens.
	tr2 := &TokenRequest{KeyName: "app.key", TTL: 3600000, Nonce: "n2"}
	tok2, _, _, _, _ := a.MintToken(tr2)
	if tok == tok2 {
		t.Errorf("tokens with different nonces should differ")
	}
}

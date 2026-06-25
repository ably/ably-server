package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
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

	t.Run("within-leeway clock skew accepted", func(t *testing.T) {
		tok := mintToken(t, testSecret, jwt.MapClaims{
			"iat": now.Add(30 * time.Second).Unix(),  // slightly future, within leeway
			"exp": now.Add(-30 * time.Second).Unix(), // slightly past, within leeway
		})
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+tok, nil)
		if _, err := a.Authenticate(r); err != nil {
			t.Fatalf("Authenticate: %v, want success within leeway", err)
		}
	})

	rejections := []struct {
		name   string
		claims jwt.MapClaims
		secret string
		alg    jwt.SigningMethod
	}{
		{name: "expired beyond leeway", claims: jwt.MapClaims{"iat": now.Add(-2 * time.Hour).Unix(), "exp": now.Add(-time.Hour).Unix()}, secret: testSecret},
		{name: "iat in future beyond leeway", claims: jwt.MapClaims{"iat": now.Add(time.Hour).Unix(), "exp": now.Add(2 * time.Hour).Unix()}, secret: testSecret},
		{name: "missing iat", claims: jwt.MapClaims{"exp": now.Add(time.Hour).Unix()}, secret: testSecret},
		{name: "missing exp", claims: jwt.MapClaims{"iat": now.Unix()}, secret: testSecret},
		{name: "bad signature", claims: valid, secret: "wrong-secret"},
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
		// "none" alg: an unsigned token must not be accepted.
		tok := jwt.NewWithClaims(jwt.SigningMethodNone, valid)
		s, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
		if err != nil {
			t.Fatalf("sign none: %v", err)
		}
		r := httptest.NewRequest(http.MethodGet, "/?access_token="+s, nil)
		if _, err := a.Authenticate(r); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("Authenticate err = %v, want ErrInvalidToken", err)
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

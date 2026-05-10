package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
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
			err := a.Authenticate(tc.req)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("Authenticate: %v, want success", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("Authenticate err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// Package auth implements credential parsing and verification for
// realtime and REST requests.
package auth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Errors returned by Authenticator.Authenticate.
var (
	ErrNoCredentials = errors.New("no credentials presented")
	ErrInvalidKey    = errors.New("invalid api key")
)

// APIKey is a parsed Ably-format API key in the form
// `appId.keyId:keySecret`.
type APIKey struct {
	AppID     string
	KeyID     string
	KeySecret string

	raw string // cached `appId.keyId:keySecret` for constant-time compare
}

// ParseAPIKey validates and decomposes an Ably-format API key. All
// three components must be non-empty.
func ParseAPIKey(s string) (APIKey, error) {
	name, secret, ok := strings.Cut(s, ":")
	if !ok {
		return APIKey{}, fmt.Errorf("api key missing ':' between name and secret")
	}
	if secret == "" {
		return APIKey{}, fmt.Errorf("api key has empty secret")
	}

	appID, keyID, ok := strings.Cut(name, ".")
	if !ok {
		return APIKey{}, fmt.Errorf("api key name missing '.' between appId and keyId")
	}
	if appID == "" {
		return APIKey{}, fmt.Errorf("api key has empty appId")
	}
	if keyID == "" {
		return APIKey{}, fmt.Errorf("api key has empty keyId")
	}

	return APIKey{
		AppID:     appID,
		KeyID:     keyID,
		KeySecret: secret,
		raw:       s,
	}, nil
}

// Authenticator verifies presented credentials against a configured API
// key.
type Authenticator struct {
	expected []byte
}

// NewAuthenticator constructs an Authenticator for the given key.
func NewAuthenticator(key APIKey) *Authenticator {
	return &Authenticator{expected: []byte(key.raw)}
}

// Authenticate extracts the presented key from r (Basic auth header
// preferred, then `key` query parameter) and verifies it against the
// configured one. Returns ErrNoCredentials if no key is presented and
// ErrInvalidKey if the credentials don't match.
func (a *Authenticator) Authenticate(r *http.Request) error {
	presented, ok := extractKey(r)
	if !ok {
		return ErrNoCredentials
	}
	if subtle.ConstantTimeCompare([]byte(presented), a.expected) != 1 {
		return ErrInvalidKey
	}
	return nil
}

// extractKey returns the presented key from a request. Basic auth wins
// over the query parameter when both are present.
func extractKey(r *http.Request) (string, bool) {
	if user, pass, ok := r.BasicAuth(); ok {
		return user + ":" + pass, true
	}
	if k := r.URL.Query().Get("key"); k != "" {
		return k, true
	}
	return "", false
}

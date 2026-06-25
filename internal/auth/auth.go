// Package auth implements credential parsing and verification for
// realtime and REST requests.
package auth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Errors returned by Authenticator.Authenticate.
var (
	ErrNoCredentials = errors.New("no credentials presented")
	ErrInvalidKey    = errors.New("invalid api key")
	ErrInvalidToken  = errors.New("invalid token")
)

// ErrClientIDMismatch is returned by ResolveClientID when the requested
// clientId is not permitted by the credential.
var ErrClientIDMismatch = errors.New("clientId not permitted by credential")

// WildcardClientID is the clientId marker (DESIGN.md §3.2) meaning the
// credential's bearer may assume any identity, choosing it per operation.
// It is never itself stamped as a message or member identity.
const WildcardClientID = "*"

// clockSkewLeeway is the tolerance applied to time-based JWT claims
// (iat, exp) to absorb small clock differences between token issuer and
// this server.
const clockSkewLeeway = 60 * time.Second

// APIKey is a parsed Ably-format API key in the form
// `appId.keyId:keySecret`.
type APIKey struct {
	AppID     string
	KeyID     string
	KeySecret string

	raw string // cached `appId.keyId:keySecret` for constant-time compare
}

// Name returns the key's `appId.keyId` portion — the value carried as a
// JWT `kid` header and as the key name in token-request paths.
func (k APIKey) Name() string {
	return k.AppID + "." + k.KeyID
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

// Method identifies how a request authenticated.
type Method int

const (
	// MethodBasic is API-key auth (Basic header or `key` query param).
	MethodBasic Method = iota
	// MethodToken is JWT bearer-token auth.
	MethodToken
)

// Principal is the result of authenticating a request: how it
// authenticated, plus any authorisation/identity claims a token carried
// for downstream resolution. Capability enforcement (TASK-12) and
// clientId resolution (TASK-11) consume these; this package only
// surfaces them.
type Principal struct {
	Method Method

	// Capability is the raw `x-ably-capability` claim (a JSON string), or
	// "" if absent. Empty for Basic auth, where the key's full capability
	// is implied.
	Capability string

	// ClientID is the `x-ably-clientId` claim; HasClientID distinguishes
	// an absent claim from a present one (including the "*" wildcard,
	// preserved verbatim). Always empty/false for Basic auth.
	ClientID    string
	HasClientID bool
}

// Authenticator verifies presented credentials against a configured API
// key.
type Authenticator struct {
	key      APIKey
	expected []byte // raw `appId.keyId:keySecret` for constant-time compare
	parser   *jwt.Parser
}

// NewAuthenticator constructs an Authenticator for the given key.
func NewAuthenticator(key APIKey) *Authenticator {
	return &Authenticator{
		key:      key,
		expected: []byte(key.raw),
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{"HS256"}),
			jwt.WithLeeway(clockSkewLeeway),
			jwt.WithIssuedAt(),         // reject iat in the future (beyond leeway)
			jwt.WithExpirationRequired(), // exp must be present
		),
	}
}

// Authenticate extracts and verifies the request's credentials. A bearer
// token (Authorization: Bearer, or the access_token / accessToken query
// param) is tried first, then an API key (Basic auth, or the `key` query
// param). Returns ErrNoCredentials if none are presented, ErrInvalidKey
// or ErrInvalidToken if verification fails.
func (a *Authenticator) Authenticate(r *http.Request) (*Principal, error) {
	if tok, ok := extractToken(r); ok {
		return a.verifyToken(tok)
	}
	if k, ok := extractKey(r); ok {
		if subtle.ConstantTimeCompare([]byte(k), a.expected) != 1 {
			return nil, ErrInvalidKey
		}
		return &Principal{Method: MethodBasic}, nil
	}
	return nil, ErrNoCredentials
}

// verifyToken verifies an HS256 JWT against the configured key's secret
// and extracts the Ably claims. With a single configured key there is no
// kid-based key selection (that arrives with multiple-key support,
// TASK-5); the signature is checked against the one secret.
func (a *Authenticator) verifyToken(tokenString string) (*Principal, error) {
	claims := jwt.MapClaims{}
	_, err := a.parser.ParseWithClaims(tokenString, claims, func(*jwt.Token) (any, error) {
		return []byte(a.key.KeySecret), nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if _, ok := claims["iat"]; !ok {
		return nil, fmt.Errorf("%w: missing iat claim", ErrInvalidToken)
	}

	p := &Principal{Method: MethodToken}
	if c, ok := claims["x-ably-capability"].(string); ok {
		p.Capability = c
	}
	if cid, ok := claims["x-ably-clientId"].(string); ok {
		p.ClientID = cid
		p.HasClientID = true
	}
	return p, nil
}

// ResolveClientID derives a connection's (or REST request's) clientId
// from the verified principal and the clientId supplied out-of-band (the
// `clientId` query param), per the DESIGN.md §3.2 table. It returns ""
// (anonymous — may assert no identity), WildcardClientID (may assume any
// identity per operation), or a concrete clientId. ErrClientIDMismatch is
// returned when the supplied param is not permitted by the credential.
func ResolveClientID(p *Principal, param string) (string, error) {
	if p.Method == MethodBasic {
		// A key holder is fully trusted and may assume any identity. With
		// no clientId param the connection is wildcard (identity chosen per
		// operation, like an Ably key); with one it is pinned to that value.
		if param == "" {
			return WildcardClientID, nil
		}
		return param, nil
	}
	// Token auth.
	if !p.HasClientID {
		// No x-ably-clientId claim: the bearer may not assert an identity.
		if param != "" {
			return "", ErrClientIDMismatch
		}
		return "", nil
	}
	if p.ClientID == WildcardClientID {
		switch param {
		case "":
			return WildcardClientID, nil // retain wildcard; identity chosen per op
		case WildcardClientID:
			return "", ErrClientIDMismatch // "*" is never a concrete identity
		default:
			return param, nil // narrow the wildcard to one identity
		}
	}
	// Concrete claim: the param must match it or be omitted.
	if param == "" || param == p.ClientID {
		return p.ClientID, nil
	}
	return "", ErrClientIDMismatch
}

// MessageClientID applies the §3.2 per-operation rule for a message or
// presence-message clientId. connClientID is the connection's resolved
// clientId (from ResolveClientID); opClientID is the clientId the
// operation carries. It returns the clientId to stamp (possibly "") and
// ok=false if the operation asserts an identity the connection may not
// use. An anonymous connection may carry no identity; a wildcard
// connection may assume any concrete identity (or none); a concrete
// connection may omit (stamped with its own) or match it.
func MessageClientID(connClientID, opClientID string) (stamped string, ok bool) {
	switch connClientID {
	case "":
		return "", opClientID == ""
	case WildcardClientID:
		if opClientID == WildcardClientID {
			return "", false
		}
		return opClientID, true
	default:
		if opClientID == "" || opClientID == connClientID {
			return connClientID, true
		}
		return "", false
	}
}

// extractToken returns a presented bearer token. The Authorization
// header (Bearer scheme) wins over the query parameters; both
// access_token (the form ably SDKs send) and accessToken are accepted.
func extractToken(r *http.Request) (string, bool) {
	if h := r.Header.Get("Authorization"); h != "" {
		if t, ok := strings.CutPrefix(h, "Bearer "); ok {
			if t = strings.TrimSpace(t); t != "" {
				return t, true
			}
		}
	}
	q := r.URL.Query()
	for _, name := range []string{"access_token", "accessToken"} {
		if t := q.Get(name); t != "" {
			return t, true
		}
	}
	return "", false
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

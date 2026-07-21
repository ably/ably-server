// Package id generates the identifiers used on the wire (connection
// IDs, message IDs).
package id

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
)

// NewConnectionID returns a fresh connection ID: 12 base64 characters
// derived from 9 random bytes (per DESIGN.md §8).
func NewConnectionID() string {
	var b [9]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand on supported platforms never returns an error.
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// ValidConnectionID reports whether s is well-formed as a connection ID
// this server could have issued: exactly the 12 base64url characters that
// encode 9 bytes (per DESIGN.md §8). Used to tell a syntactically valid
// resume/recover key (best-effort continuation) from a malformed one that
// must be declined per protocol (DESIGN.md §4.3).
func ValidConnectionID(s string) bool {
	if len(s) != 12 {
		return false
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	return err == nil && len(b) == 9
}

// connectionKeySuffixBytes is the byte length of the HMAC-SHA256 suffix
// appended to a connectionId to form its connectionKey — truncated to
// keep the wire key compact (24 base64url chars total, matching
// connectionId's own 12-char length), while remaining infeasible to
// forge without the server's secret.
const connectionKeySuffixBytes = 9

// NewConnectionKey returns the connectionKey for connID: connID followed
// by a truncated HMAC-SHA256 over connID, keyed with secret (DESIGN.md
// §8). A connectionKey is more than the bare connectionId so that a
// bearer of just the connectionId — e.g. observed on a delivered
// Message.connectionId — cannot replay it as a resume/recover key for a
// connection it doesn't own; only VerifyConnectionKey, with the same
// secret, can confirm the pairing.
func NewConnectionKey(secret []byte, connID string) string {
	return connID + connectionKeySuffix(secret, connID)
}

// VerifyConnectionKey reports whether key is a connectionKey this server
// (holding secret) could have issued, and if so returns the connectionId
// it authenticates. Used to decide whether a resume/recover key retains
// its connectionId (DESIGN.md §4.3, §8) — connection-state resume itself
// stays a non-goal; this only authenticates the identity, not any state.
func VerifyConnectionKey(secret []byte, key string) (connID string, ok bool) {
	if len(key) != 12+base64.RawURLEncoding.EncodedLen(connectionKeySuffixBytes) {
		return "", false
	}
	connID, gotSuffix := key[:12], key[12:]
	if !ValidConnectionID(connID) {
		return "", false
	}
	wantSuffix := connectionKeySuffix(secret, connID)
	if subtle.ConstantTimeCompare([]byte(gotSuffix), []byte(wantSuffix)) != 1 {
		return "", false
	}
	return connID, true
}

func connectionKeySuffix(secret []byte, connID string) string {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(connID))
	sum := h.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(sum[:connectionKeySuffixBytes])
}

// NewMessageBaseID returns a fresh message-publish batch id: 8 base64
// characters derived from 6 random bytes (per DESIGN.md §8). It is the
// server-generated idempotency key stamped onto a ChannelMessage whose
// publisher supplied none, with each contained Message.ID set to
// "<baseID>:<idx>".
func NewMessageBaseID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand on supported platforms never returns an error.
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

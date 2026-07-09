// Package id generates the identifiers used on the wire (connection
// IDs, message IDs).
package id

import (
	"crypto/rand"
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

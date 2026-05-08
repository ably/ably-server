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

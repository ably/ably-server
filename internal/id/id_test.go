package id

import "testing"

func TestNewConnectionIDIsValid(t *testing.T) {
	got := NewConnectionID()
	if !ValidConnectionID(got) {
		t.Errorf("NewConnectionID() = %q, not ValidConnectionID", got)
	}
}

func TestConnectionKeyRoundTrip(t *testing.T) {
	secret := []byte("test-secret")
	connID := NewConnectionID()
	key := NewConnectionKey(secret, connID)

	if key == connID {
		t.Fatalf("NewConnectionKey(%q) = %q, want more than the bare connectionId", connID, key)
	}
	got, ok := VerifyConnectionKey(secret, key)
	if !ok || got != connID {
		t.Fatalf("VerifyConnectionKey(%q) = (%q, %v), want (%q, true)", key, got, ok, connID)
	}
}

func TestVerifyConnectionKeyRejectsBareConnectionID(t *testing.T) {
	secret := []byte("test-secret")
	connID := NewConnectionID()
	// A bare connectionId — e.g. one observed on a delivered Message — must
	// not be accepted as a resume/recover key on its own.
	if _, ok := VerifyConnectionKey(secret, connID); ok {
		t.Errorf("VerifyConnectionKey accepted the bare connectionId %q", connID)
	}
}

func TestVerifyConnectionKeyRejectsTamperedSuffix(t *testing.T) {
	secret := []byte("test-secret")
	connID := NewConnectionID()
	key := NewConnectionKey(secret, connID)
	tampered := key[:len(key)-1] + flipChar(key[len(key)-1])
	if _, ok := VerifyConnectionKey(secret, tampered); ok {
		t.Errorf("VerifyConnectionKey accepted a tampered suffix %q", tampered)
	}
}

func TestVerifyConnectionKeyRejectsWrongSecret(t *testing.T) {
	connID := NewConnectionID()
	key := NewConnectionKey([]byte("secret-a"), connID)
	if _, ok := VerifyConnectionKey([]byte("secret-b"), key); ok {
		t.Errorf("VerifyConnectionKey(wrong secret) accepted %q", key)
	}
}

func TestVerifyConnectionKeyRejectsMalformedConnectionID(t *testing.T) {
	secret := []byte("test-secret")
	// Same total length as a real key, but the leading 12 chars don't
	// decode to a 9-byte connectionId.
	key := NewConnectionKey(secret, "not-a-valid!")
	if _, ok := VerifyConnectionKey(secret, key); ok {
		t.Errorf("VerifyConnectionKey accepted a malformed connectionId prefix %q", key)
	}
}

func TestVerifyConnectionKeyRejectsWrongLength(t *testing.T) {
	secret := []byte("test-secret")
	for _, key := range []string{"", "short", NewConnectionID(), NewConnectionKey(secret, NewConnectionID()) + "x"} {
		if _, ok := VerifyConnectionKey(secret, key); ok {
			t.Errorf("VerifyConnectionKey(%q) accepted, want rejected on length", key)
		}
	}
}

func flipChar(b byte) string {
	if b == 'A' {
		return "B"
	}
	return "A"
}

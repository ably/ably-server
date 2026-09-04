package handles

import (
	"testing"

	"github.com/ably/ably-server/internal/logging"

	protoauth "github.com/ably/server-protocol/go/auth"
	"github.com/ably/server-protocol/go/conf"
)

// TestConnectionKey_MintAndVerify drives the connection key this server would
// hand a client and take back on a resume, through the shared auth manager.
//
// A connection key carries the connection id, signed, plus the site that
// signed it — which is why resuming works without anywhere to look the
// connection up. That matters here: this server has no store to recover a
// connection from, so a key that is not self-contained could not be resumed
// at all.
func TestConnectionKey_MintAndVerify(t *testing.T) {
	c := conf.Default()
	c.Auth.ConnectionMacKey.Store([]byte("a-secret-for-signing-connection-keys"))
	authMgr := NewAuthManager(c.Auth, logging.Default())

	const (
		connID   = "abcdefghij"
		appID    = "ably-server"
		clientID = "alice"
	)

	key := authMgr.ConnectionKey(connID, appID, clientID)
	t.Logf("connection key = %q", key)

	composite, err := protoauth.ParseCompositeConnectionKey(key)
	if err != nil {
		t.Fatalf("the key this server minted does not parse: %s", err)
	}

	got, errInfo := authMgr.VerifyConnectionKey(composite, appID, clientID, protoauth.ConnKeyVerifyOpts{
		RequireSelfContainedKey: true,
		CurrentSiteOnly:         true,
	})
	if errInfo != nil {
		t.Fatalf("the key this server minted does not verify: %s", errInfo)
	}
	if got != connID {
		t.Errorf("verified connection id = %q, want %q", got, connID)
	}
}

// TestConnectionKey_RefusesAnotherClient checks that a key is bound to the
// client it was issued to, so one client cannot resume another's connection.
func TestConnectionKey_RefusesAnotherClient(t *testing.T) {
	c := conf.Default()
	c.Auth.ConnectionMacKey.Store([]byte("a-secret-for-signing-connection-keys"))
	authMgr := NewAuthManager(c.Auth, logging.Default())

	key := authMgr.ConnectionKey("abcdefghij", "ably-server", "alice")
	composite, err := protoauth.ParseCompositeConnectionKey(key)
	if err != nil {
		t.Fatalf("parse: %s", err)
	}

	if _, errInfo := authMgr.VerifyConnectionKey(composite, "ably-server", "mallory", protoauth.ConnKeyVerifyOpts{
		RequireSelfContainedKey: true,
		CurrentSiteOnly:         true,
	}); errInfo == nil {
		t.Error("a key issued to alice verified for mallory")
	}
}

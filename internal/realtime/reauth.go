package realtime

import (
	"context"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/protocol"
)

// preExpiryWindow is how long before a token's expiry the server sends an
// AUTH frame prompting the client to supply a fresh token (DESIGN.md §3).
// It gives the client time to renew before the hard expiry disconnect.
// Matches the reference server's WarnBeforeExpiryTime.
const preExpiryWindow = 30 * time.Second

// noWarningMargin is the minimum remaining token lifetime for which an AUTH
// prompt is worth sending. A token adopted with less life than this is not
// prompted at all — it simply expires and the connection is disconnected
// (renewable), which the SDK reconnects through (RTN22a). Prompting such a
// short-lived token would race its own expiry and, if the client renewed with
// another equally short token, spin an inband-renew loop instead of the
// disconnect/reconnect the SDK expects. Matches the reference server's
// NoWarningMargin (DESIGN.md §3).
const noWarningMargin = 5500 * time.Millisecond

// capability returns the connection's current capability set, which inband
// re-auth may replace (DESIGN.md §3).
func (c *connection) capability() auth.Capability {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.cap
}

// initialTokenExpiry returns the token expiry to seed authLoop with. Read
// under authMu: an inband reauth (setAuth) can race an early call to this
// from the connection's startup goroutine otherwise.
func (c *connection) initialTokenExpiry() time.Time {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.tokenExpiry
}

// setAuth replaces the connection's capability set and token expiry after a
// successful inband re-auth.
func (c *connection) setAuth(cap auth.Capability, expiry time.Time) {
	c.authMu.Lock()
	c.cap = cap
	c.tokenExpiry = expiry
	c.authMu.Unlock()
}

// authLoop enforces the connection's token expiry (DESIGN.md §3).
// For a token whose remaining lifetime exceeds noWarningMargin it sends an
// AUTH prompt preExpiryWindow ahead of expiry (immediately if that instant is
// already past); a token with less life than the margin is left to expire
// without a prompt. Either way, if no valid re-auth arrives by expiry the
// connection is disconnected with a token-expired error. A successful inband
// AUTH signals the new expiry over c.reauth, which reschedules. A zero expiry
// (Basic auth) leaves the loop idle until a re-auth supplies one.
func (c *connection) authLoop(ctx context.Context, expiry time.Time) {
	for {
		if expiry.IsZero() {
			select {
			case <-ctx.Done():
				return
			case expiry = <-c.reauth:
				continue
			}
		}

		// Prompt phase: for a token with enough life left, wait until
		// preExpiryWindow before expiry (or fire at once if that is already
		// past) and ask the client to renew. Shorter-lived tokens skip
		// straight to the expiry phase (RTN22a).
		if time.Until(expiry) > noWarningMargin {
			prompt := time.NewTimer(time.Until(expiry.Add(-preExpiryWindow)))
			select {
			case <-ctx.Done():
				prompt.Stop()
				return
			case expiry = <-c.reauth:
				prompt.Stop()
				continue
			case <-prompt.C:
				c.queue(ctx, &protocol.ProtocolMessage{Action: protocol.ActionAuth})
			}
		}

		// Expiry phase: wait for the hard expiry. A re-auth before then
		// reschedules; otherwise the connection is disconnected.
		hard := time.NewTimer(time.Until(expiry))
		select {
		case <-ctx.Done():
			hard.Stop()
			return
		case expiry = <-c.reauth:
			hard.Stop()
			continue
		case <-hard.C:
			c.disconnectExpired(ctx)
			return
		}
	}
}

// disconnectExpired sends a DISCONNECTED frame carrying the token-expired
// error (40142 / 401) and lets the write loop close the socket, which
// unblocks the read loop and drives normal teardown. SDKs treat this
// as a renewable token error and reconnect (DESIGN.md §3).
func (c *connection) disconnectExpired(ctx context.Context) {
	c.queue(ctx, &protocol.ProtocolMessage{
		Action: protocol.ActionDisconnected,
		Error: &protocol.ErrorInfo{
			Message:    "token expired",
			Code:       40142,
			StatusCode: 401,
		},
	})
}

// handleAuth processes an inbound AUTH frame carrying a fresh token for
// inband re-authentication (DESIGN.md §3). It verifies the token,
// requires the resulting identity to be compatible with the connection's
// (the §3.2 clientId must not change), swaps in the new capability set and
// expiry, and replies with a CONNECTED frame carrying updated
// ConnectionDetails. An invalid or incompatible token disconnects the
// connection per protocol.
func (c *connection) handleAuth(ctx context.Context, msg *protocol.ProtocolMessage) {
	if msg.Auth == nil || msg.Auth.AccessToken == "" {
		c.failReauth(ctx, "AUTH frame carried no access token", 40101)
		return
	}
	p, err := c.authn.VerifyToken(msg.Auth.AccessToken)
	if err != nil {
		c.logger.Warn("inband auth: token verification failed", "err", err)
		c.failReauth(ctx, "invalid token", 40101)
		return
	}
	// Inband AUTH carries no clientId query param; the identity comes from
	// the token alone and must match the connection's established identity.
	newClientID, err := auth.ResolveClientID(p, "")
	if err != nil || newClientID != c.clientID {
		c.logger.Warn("inband auth: incompatible clientId",
			"connClientId", c.clientID, "newClientId", newClientID, "err", err)
		c.failReauth(ctx, "token clientId is incompatible with the connection", 40102)
		return
	}

	c.setAuth(p.Capabilities(), p.ExpiresAt)
	c.reconcileAttachmentCapabilities(ctx)

	// Signal the authLoop to reschedule against the new expiry. The
	// channel is buffered (cap 1) so this never blocks the read loop.
	select {
	case c.reauth <- p.ExpiresAt:
	case <-ctx.Done():
		return
	}

	c.queue(ctx, &protocol.ProtocolMessage{
		Action:            protocol.ActionConnected,
		ConnectionID:      c.id,
		ConnectionDetails: c.connectionDetails(),
	})
}

// failReauth rejects an inband re-auth attempt with an ERROR frame carrying
// the given error code (401), which the write loop flushes before closing the
// socket. Unlike an expired token (disconnectExpired, a renewable DISCONNECTED
// 40142 that the SDK reconnects through), a credential the client supplied
// itself and that the server rejected is not renewable: the code is outside
// the SDK's renewable token-error range (40140–40149), so an ERROR frame moves
// the connection to FAILED rather than triggering a reconnect loop (RTC8a2,
// DESIGN.md §3).
func (c *connection) failReauth(ctx context.Context, message string, code int) {
	c.queue(ctx, &protocol.ProtocolMessage{
		Action: protocol.ActionError,
		Error: &protocol.ErrorInfo{
			Message:    message,
			Code:       code,
			StatusCode: 401,
		},
	})
}

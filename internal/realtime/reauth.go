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
const preExpiryWindow = 30 * time.Second

// capability returns the connection's current capability set, which inband
// re-auth may replace (DESIGN.md §3, TASK-17).
func (c *connection) capability() auth.Capability {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.cap
}

// setAuth replaces the connection's capability set and token expiry after a
// successful inband re-auth.
func (c *connection) setAuth(cap auth.Capability, expiry time.Time) {
	c.authMu.Lock()
	c.cap = cap
	c.tokenExpiry = expiry
	c.authMu.Unlock()
}

// authLoop enforces the connection's token expiry (DESIGN.md §3, TASK-17).
// While a token expiry is set it sends an AUTH prompt preExpiryWindow ahead
// of expiry and, if no valid re-auth arrives by expiry, disconnects with a
// token-expired error. A successful inband AUTH signals the new expiry over
// c.reauth, which reschedules. A zero expiry (Basic auth) leaves the loop
// idle until a re-auth supplies one.
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

		// Prompt phase: wait until preExpiryWindow before expiry, then ask
		// the client to renew.
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
// unblocks the read loop and drives normal teardown. ably-go treats this
// as a renewable token error and reconnects (DESIGN.md §3).
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
// inband re-authentication (DESIGN.md §3, TASK-17). It verifies the token,
// requires the resulting identity to be compatible with the connection's
// (the §3.2 clientId must not change), swaps in the new capability set and
// expiry, and replies with a CONNECTED frame carrying updated
// ConnectionDetails. An invalid or incompatible token disconnects the
// connection per protocol.
func (c *connection) handleAuth(ctx context.Context, msg *protocol.ProtocolMessage) {
	if msg.Auth == nil || msg.Auth.AccessToken == "" {
		c.failReauth(ctx, "AUTH frame carried no access token", 40140)
		return
	}
	p, err := c.authn.VerifyToken(msg.Auth.AccessToken)
	if err != nil {
		c.logger.Warn("inband auth: token verification failed", "err", err)
		c.failReauth(ctx, "invalid token", 40140)
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

// failReauth rejects an inband re-auth attempt with a DISCONNECTED frame
// carrying the given error code (401), which the write loop flushes before
// closing the socket.
func (c *connection) failReauth(ctx context.Context, message string, code int) {
	c.queue(ctx, &protocol.ProtocolMessage{
		Action: protocol.ActionDisconnected,
		Error: &protocol.ErrorInfo{
			Message:    message,
			Code:       code,
			StatusCode: 401,
		},
	})
}

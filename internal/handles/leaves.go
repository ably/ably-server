package handles

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/ably/server-protocol/go/channel"
	protocollog "github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/wire"
)

// delayedLeaves holds a connection's presence leave back for a while, so that
// a client whose connection drops and reconnects inside its remainPresentFor
// window never appears to its fellow members to have left.
//
// A leave that is never cancelled must still happen: a member whose connection
// is gone for good has to leave, or the channel keeps them present forever.
// That is what the timer is for, and why cancelling is by connection id — the
// same connection coming back is the only thing that means "they never left".
type delayedLeaves struct {
	ctx     context.Context
	publish func(context.Context, *wire.ChannelMessage)
	log     protocollog.Logger

	mu      sync.Mutex
	pending map[string]*time.Timer
}

func newDelayedLeaves(ctx context.Context, publish func(context.Context, *wire.ChannelMessage), log protocollog.Logger) *delayedLeaves {
	return &delayedLeaves{
		ctx:     ctx,
		publish: publish,
		log:     log,
		pending: make(map[string]*time.Timer),
	}
}

// hold schedules leave for connID, replacing any leave already held for it.
//
// ref is the reference that keeps the channel alive while the leave is
// outstanding: a channel collected before its held leave fired would leave the
// member present for good. It is kept alive by the timer's closure rather than
// stored, which is all a counted reference asks of a holder.
func (d *delayedLeaves) hold(leave *wire.ChannelMessage, connID string, delay time.Duration, ref channel.Ref) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if existing, ok := d.pending[connID]; ok {
		existing.Stop()
	}
	d.log.Debug("holding a connection's presence leave", "connectionId", connID, "delay", delay)
	d.pending[connID] = time.AfterFunc(delay, func() {
		defer runtime.KeepAlive(ref)

		d.mu.Lock()
		delete(d.pending, connID)
		d.mu.Unlock()

		if d.ctx.Err() != nil {
			return
		}
		d.log.Debug("a held presence leave was never cancelled; publishing it", "connectionId", connID)
		d.publish(d.ctx, leave)
	})
}

// cancel drops the leave held for connID, because the connection came back.
func (d *delayedLeaves) cancel(connID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if timer, ok := d.pending[connID]; ok {
		d.log.Debug("a connection returned before its presence leave was due", "connectionId", connID)
		timer.Stop()
		delete(d.pending, connID)
	}
}

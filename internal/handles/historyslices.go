package handles

import (
	"context"

	"github.com/ably/ably-server/internal/storage"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/errors"
	"github.com/ably/server-protocol/go/wire"
)

// HistorySlices is where the message cache gets what it does not hold: the run
// of this channel's history older than its earliest entry.
//
// A query is always bounded above by a serial — where the cache's own entries
// begin, so that a fetch cannot return a message the live tail is about to
// deliver anyway. Below it is bounded by whatever the client asked in: a
// serial for a resume, a moment for a rewind by time, and for a rewind by
// count nothing at all, only how many.
//
// This is the whole of what the cache asks of this server. Which messages fall
// within the query is answered out of storage; what the client is then told
// about them — where a rewind lands, how far back a resume may reach, what it
// is told when it asked for further — is the cache's.
func (c *liveChannel) HistorySlices(ctx context.Context, r channel.HistoryRange) (*channel.HistoryPage, *errors.ErrorInfo) {
	// Read backwards from the upper bound, so that when the limit cuts it is
	// the oldest messages that are left out rather than the newest — the cache
	// wants the run adjoining the live edge. One more than asked for, because
	// the extra oldest message is what says where the run begins.
	sq := storage.HistoryQuery{
		Kind:      storage.KindMessage,
		Direction: storage.DirectionBackwards,
		Limit:     r.Limit + 1,
	}
	if r.To != nil {
		sq.EndChannelSerial = r.To.ToTimeserialString()
	}
	switch {
	case r.From == nil:
		// A rewind by count, bounded by the limit and nothing else.
	case r.From.SeriesId == "":
		// A serial carrying only a time is a rewind by time: what is being
		// asked for is everything published since that moment.
		sq.Start = int64(r.From.Time)
	default:
		// A resume, from the serial the client last saw.
		sq.AfterChannelSerial = r.From.ToTimeserialString()
	}

	page, err := c.ch.History(ctx, sq)
	if err != nil {
		return nil, errors.New(50000, 500, "unable to read the channel's history: %s", err)
	}

	// Storage answered newest-first; the cache reads a slice in stream order.
	messages := make([]*wire.ChannelMessage, 0, len(page.ChannelMessages))
	for i := len(page.ChannelMessages) - 1; i >= 0; i-- {
		messages = append(messages, channelMessage(page.ChannelMessages[i]))
	}

	preSerial, messages, errInfo := c.historyBegins(ctx, r, messages)
	if errInfo != nil {
		return nil, errInfo
	}
	return &channel.HistoryPage{Slices: []*channel.HistorySlice{{
		PreSerial: preSerial,
		Messages:  messages,
	}}}, nil
}

// historyBegins says where the run starts: the serial a client attaches at to
// receive the whole of it, and the run itself with that boundary message
// removed.
//
// The boundary matters because it is what a resume is matched against. Put it
// too late and a client that could have resumed is told its continuity broke;
// put it too early and a client is told it resumed cleanly from a point whose
// successors this server no longer holds.
func (c *liveChannel) historyBegins(ctx context.Context, r channel.HistoryRange, messages []*wire.ChannelMessage) (*wire.Timeserial, []*wire.ChannelMessage, *errors.ErrorInfo) {
	// The limit cut the run, so the message that fell off the oldest end is
	// the boundary: everything after it is here, and nothing before it is.
	if len(messages) > r.Limit {
		return messages[0].ChannelSerial, messages[1:], nil
	}

	// Nothing was cut, so the run reaches as far back as the query allowed. A
	// resume reaches exactly to the serial presented — provided the channel
	// still holds something at or before it, which is the difference between a
	// resume point this server can serve from and one whose predecessors it
	// has since dropped.
	if r.From != nil && r.From.SeriesId != "" {
		page, err := c.ch.History(ctx, storage.HistoryQuery{
			Kind:             storage.KindMessage,
			Direction:        storage.DirectionBackwards,
			EndChannelSerial: r.From.ToTimeserialString(),
			Limit:            1,
		})
		if err != nil {
			return nil, nil, errors.New(50000, 500, "unable to read the channel's history: %s", err)
		}
		if len(page.ChannelMessages) > 0 {
			serial, _ := wire.TimeserialFromString(page.ChannelMessages[0].ChannelSerial)
			return serial, messages, nil
		}
	}

	// The run reaches the beginning of the channel, which is the one serial
	// that always sorts before every message on it.
	initial, _ := wire.TimeserialFromString(c.ch.InitialChannelSerial())
	return initial, messages, nil
}

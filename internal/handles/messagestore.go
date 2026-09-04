package handles

import (
	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/handles"
)

// messageStore is one channel's persisted history, as a request reads it out
// of this server's storage.
//
// Every method here is a read of what was kept, and none of them brings the
// channel live: what a channel is holding now is the message cache's, reached
// through the channel itself. The protocol code decides what a client asked
// for and what it may see, and asks this for the messages. Which store the
// answer comes out of, and how it is indexed, is this server's business.
type messageStore struct {
	app  *appHandle
	spec *channel.Spec
}

var _ handles.MessageStore = (*messageStore)(nil)

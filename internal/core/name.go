package core

import (
	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/resource"
	"github.com/ably/server-protocol/go/scope"
)

// nameScope is the scope channel names are parsed under. Which app a channel
// belongs to does not change whether its name is well-formed, and this server
// serves one app, so the scope is a constant.
var nameScope = scope.New(scope.App, "ably-server")

// ValidChannelName reports whether name is an acceptable channel name.
//
// The grammar is the protocol's, so this asks the shared parser rather than
// restating it. An invalid name is rejected with Ably error 40010 at ATTACH
// and at publish, on both the realtime and REST surfaces (DESIGN.md §4).
//
// A qualified name is rejected, which is a limitation rather than a decision.
// A qualifier says which channel a name refers to and how to attach to it —
// `[?rewind=1]foo` is channel `foo` attached with rewind — and this server
// does not yet route that: it would take the whole string as the channel
// name, so a client asking for rewind this way would land on a channel of its
// own rather than on `foo`. Note that rewind itself is served, through the
// params field of an ATTACH, so what is refused here is one of the two ways
// of asking for something this server does. Routing the qualifier into the
// attach is what lifts this, and comes with the shared attachment.
func ValidChannelName(name string) bool {
	spec, errInfo := channel.ParseSpec(name, nameScope)
	if errInfo != nil {
		return false
	}
	qualifier := spec.Qualifier()
	return qualifier.Type == resource.QualifierDefault && len(qualifier.Query) == 0
}

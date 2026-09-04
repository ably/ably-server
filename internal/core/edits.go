package core

import (
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/wire"
)

// MergeVersion and FoldSummary are what this server hands storage when it
// makes an edit or an annotation: the shared module's answers, so that what an
// edit does to a message and what an annotation does to a summary are the same
// here as anywhere else the protocol is spoken.
//
// They live at this layer because storage is not allowed to know them — it
// runs them, with the target loaded and the write not yet made, but what they
// mean is not its business (see storage.MergeFunc).

// MergeVersion turns an update, delete or append into the version to store.
//
// maxMessageSize is the ceiling an aggregated append has to fit under; zero
// leaves it unenforced.
func MergeVersion(maxMessageSize int64, log logging.Logger) storage.MergeFunc {
	return func(current, mut *wire.Message, versionSerial string) (*wire.Message, error) {
		// Who performed the operation, which is not necessarily who published
		// the message being operated on: a client may edit another's message,
		// and the operation says so itself. This is read before the merge,
		// which replaces the edit's clientId with the original publisher's.
		if mut.Version == nil {
			mut.Version = &wire.Message_Version{}
		}
		if mut.Version.ClientId == nil {
			mut.Version.ClientId = mut.ClientId
		}

		if err := channel.BuildUpdateMessage(current, mut, nil, maxMessageSize, log); err != nil {
			return nil, &storage.ProtocolError{Info: err}
		}

		// The version's own position, which only storage knew: it is the
		// serial the edit was minted at, and the timestamp it encodes is when
		// the edit happened (the message's own timestamp stays its create
		// time).
		ts, _ := serial.Timestamp(versionSerial)
		mut.Version.Serial = versionSerial
		mut.Version.Timestamp = uint64(ts)

		// An append is stored and fanned out as the rolled-up aggregate, with
		// the incremental delta alongside it in Alt — and it is the delta a
		// caught-up subscriber is given. The merge clones that delta before
		// this point, so it has to be told the version too: the delta and the
		// aggregate are one version of the message, expressed two ways, and a
		// subscriber decides what it has already applied by version. A delta
		// left unversioned reads as older than the create it follows, so an
		// SDK folding a stream discards every chunk and the message stops
		// growing.
		if delta := mut.Alt[wire.DeltaAppend]; delta != nil {
			if delta.Version == nil {
				delta.Version = &wire.Message_Version{}
			}
			delta.Version.Serial = mut.Version.Serial
			delta.Version.Timestamp = mut.Version.Timestamp
		}
		return mut, nil
	}
}

// FoldSummary folds one annotation into the summary of the message it
// annotates, reporting whether the summary changed.
func FoldSummary(log logging.Logger) storage.FoldFunc {
	return func(msg *wire.Message, a *wire.Annotation) bool {
		return channel.ApplyAnnotation(msg, a, log)
	}
}

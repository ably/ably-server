package core

import (
	"github.com/ably/ably-server/internal/storage"

	"github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/state/liveslice"
	"github.com/ably/server-protocol/go/wire"
)

// ApplyOperations is what this server hands storage when it stores a
// LiveObjects publish: the shared module's answer to what an operation does to
// an object, so that the set this server materialises is the one the protocol
// says it is (DESIGN.md §15.3).
//
// It sits at the same seam as MergeVersion and FoldSummary, for the same
// reason: storage runs it, with the objects loaded and the write not yet made,
// but what an operation means is not storage's business (see
// storage.ApplyFunc). Here the module's own machinery answers it — a LiveSlice
// is exactly a pool of objects that operations are applied to, which is what a
// materialised set is.
//
// The slice is built per call rather than kept, because the authoritative set
// is the stored one: a resident slice would be a second copy to keep in step
// with it, and on a cluster it would be a stale one. Building it costs only
// the objects the publish names — an operation only ever changes the object it
// names, so those are the only ones storage loads and the only ones this can
// return.
func ApplyOperations(log logging.Logger) storage.ApplyFunc {
	return func(objects []*wire.StateObject, state []*wire.StateMessage) ([]*wire.StateObject, error) {
		slice := liveslice.NewLiveSlice(log, objects...)

		// Which objects changed, in the order the publish first changed them.
		// An operation that is not applied — one the object has already seen a
		// later operation from the same site than — changes nothing, and an
		// object nothing changed must not be written back over a concurrent
		// writer's version of it.
		var changed []string
		seen := make(map[string]bool, len(state))
		for _, sm := range state {
			op := sm.GetOperation()
			if op == nil || op.GetObjectId() == "" {
				// An object rather than an operation: only the server puts
				// those on the wire, so a publish carrying one has nothing to
				// apply.
				continue
			}
			if !slice.Put(op, sm.GetSerial()) {
				continue
			}
			if id := op.GetObjectId(); !seen[id] {
				seen[id] = true
				changed = append(changed, id)
			}
		}

		out := make([]*wire.StateObject, 0, len(changed))
		for _, id := range changed {
			// The object as it now stands, not as any one operation left it: a
			// publish may carry several operations on the same object, and
			// what storage writes back is where they ended up.
			if obj := slice.GetObjectByID(id); obj != nil {
				out = append(out, obj.StateObject())
			}
		}
		return out, nil
	}
}

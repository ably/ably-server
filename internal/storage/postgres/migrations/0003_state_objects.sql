-- Materialised LiveObjects state (DESIGN.md §15.3): one row per object
-- on a channel, holding the object as the channel's state stream has
-- left it.
--
-- It is the objects analogue of the presence table, and is maintained
-- the same way — upserted in the same transaction as the state cm's
-- INSERT into channel_messages, so the set never disagrees with the log
-- it is the fold of. It lives in Postgres rather than in memory (as the
-- single-process backends' presence set does) for the same reason as
-- presence: in cluster mode it has to be globally authoritative, since
-- the node serving a client's state sync is not necessarily the node
-- that applied the operations.
--
-- Unlike presence there is no liveness lease. A presence row is owned by
-- a connection and orphaned when the node holding it dies; an object is
-- owned by the channel and outlives every connection that touched it, so
-- there is nothing for a reaper to collect. An object stops existing
-- only when an operation says so — a tombstone, which is a value in the
-- payload, not the absence of a row.
--
-- The PK is (channel, object_id), which also gives the ordered scan a
-- state sync pages with: object id order is the order the protocol's own
-- paging walks the set in, and the sync cursor is an object id.
CREATE TABLE state_objects (
  channel        TEXT  NOT NULL,
  object_id      TEXT  NOT NULL,
  channel_serial TEXT  NOT NULL,  -- serial of the operation that last changed it
  payload        BYTEA NOT NULL,  -- protobuf-encoded wire.StateObject
  PRIMARY KEY (channel, object_id)
);

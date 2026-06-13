-- Materialised current PRESENCE state: one row per live member, keyed
-- by (connectionId, clientId). ENTER/UPDATE upsert, LEAVE deletes —
-- maintained in the same transaction as the presence cm's INSERT into
-- channel_messages (DESIGN.md §12.5), so the set stays consistent with
-- the log. Held in Postgres (not in-memory like the single-process
-- backends) so it is globally authoritative across cluster nodes.
--
-- The crashed-node reaper columns (node_id, expires_at) are a later
-- task (TASK-47); the base set is maintained by clean LEAVEs on
-- connection teardown.

CREATE TABLE presence (
  channel        TEXT  NOT NULL,
  connection_id  TEXT  NOT NULL,
  client_id      TEXT  NOT NULL,
  channel_serial TEXT  NOT NULL,  -- serial of the latest ENTER/UPDATE
  payload        BYTEA NOT NULL,  -- msgpack-encoded protocol.PresenceMessage
  PRIMARY KEY (channel, connection_id, client_id)
);

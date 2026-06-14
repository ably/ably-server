-- Mutable messages (DESIGN.md §13). A mutation (update/delete/append)
-- is a fresh publish that references a prior message's identity, plus
-- two derived structures the server materialises so reads collapse the
-- version chain without replaying it.

-- channel_messages gains message_serial: the stable message IDENTITY a
-- row is a version of. For a message create it is the row's own
-- Message.serial; for a mutation it is the target's serial. NULL for
-- presence rows (presence has no version chain). The partial index over
-- (channel, message_serial, channel_serial, idx) is the serial→versions
-- index: it backs the by-serial version scan in version order and the
-- update/delete merge-against-target lookup.
ALTER TABLE channel_messages ADD COLUMN message_serial TEXT;

CREATE INDEX channel_messages_versions_idx
  ON channel_messages (channel, message_serial, channel_serial, idx)
  WHERE message_serial IS NOT NULL;

-- messages is the materialised latest-version projection: one row per
-- message identity holding the latest merged Message (DESIGN.md §13.4).
-- A delete leaves the row with deleted = TRUE (a tombstone) rather than
-- removing it, so the message and its versions stay queryable. Backs
-- GET .../messages/{serial} and the collapsed default history, which
-- orders by message_serial (== create serial, so the latest content sits
-- at the message's original timeline position).
CREATE TABLE messages (
  channel        TEXT    NOT NULL,
  message_serial TEXT    NOT NULL,  -- stable identity (== the create's Message.serial)
  payload        BYTEA   NOT NULL,  -- msgpack-encoded latest merged protocol.Message
  deleted        BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (channel, message_serial)
);

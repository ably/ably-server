-- Schema for the Postgres storage backend (DESIGN.md §6.3).
--
-- One row per individual Message. The PK groups Messages under their
-- shared channelSerial; the channelSerial prefix encodes the mint
-- timestamp (§8), so a forward range scan over the PK covers both
-- ordered history reads and time-based retention without a separate
-- created_at column.
--
-- The CREATE is IF NOT EXISTS so Open() can safely reapply on an
-- already-bootstrapped database. Concurrent-safe schema application
-- across N booting nodes is TASK-23's responsibility (an advisory
-- lock around the whole sequence) and lives outside this file.

CREATE TABLE IF NOT EXISTS messages (
  channel        TEXT  NOT NULL,
  channel_serial TEXT  NOT NULL,
  idx            INT   NOT NULL,
  id             TEXT,
  payload        BYTEA NOT NULL,
  PRIMARY KEY (channel, channel_serial, idx)
);

-- Idempotency: a non-NULL client-supplied Message.id must be unique
-- per channel within the retention window. The partial index skips
-- NULL ids so publishes without an id never collide.
CREATE UNIQUE INDEX IF NOT EXISTS messages_idempotency_idx
  ON messages (channel, id)
  WHERE id IS NOT NULL;

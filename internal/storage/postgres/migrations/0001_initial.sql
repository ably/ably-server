-- Initial schema for the Postgres storage backend (DESIGN.md §6.3).
--
-- channel_messages is the append-only LOG: one row per individual
-- Message OR PresenceMessage, both kinds interleaved in one
-- channelSerial namespace (§12.1). The PK groups a publish's items
-- under their shared channelSerial; the channelSerial prefix encodes
-- the mint timestamp (§8), so a forward range scan over the PK covers
-- both ordered history reads and time-based retention without a
-- separate created_at column. action lives in the payload (nothing
-- scans by it); kind is a column because reads filter by it.

CREATE TABLE channel_messages (
  channel        TEXT  NOT NULL,
  channel_serial TEXT  NOT NULL,
  idx            INT   NOT NULL,
  id             TEXT,
  kind           TEXT  NOT NULL DEFAULT 'message',  -- 'message' | 'presence' (§12.1)
  payload        BYTEA NOT NULL,
  PRIMARY KEY (channel, channel_serial, idx)
);

-- Idempotency: a non-NULL client-supplied id must be unique per channel
-- within the retention window (shared across message and presence
-- kinds). The partial index skips NULL ids so publishes without an id
-- never collide.
CREATE UNIQUE INDEX channel_messages_idempotency_idx
  ON channel_messages (channel, id)
  WHERE id IS NOT NULL;

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
--
-- message_serial is the stable message IDENTITY a row is a version of
-- (DESIGN.md §13): for a message create it is the row's own
-- Message.serial; for a mutation (update/delete/append) it is the
-- target's serial. NULL for presence rows (presence has no version
-- chain).
--
-- is_append marks rows persisted via a streamed append (DESIGN.md
-- §13.3): an append is stored as a full action=update version
-- carrying the rolled-up aggregate, so the log stays append-only and
-- live/resume fan-out sees every append, while the version-history
-- read-path collapses a run of appends to its aggregate rather than
-- enumerate each delta.
--
-- summary holds the msgpack-encoded post-fold annotation-summary
-- SNAPSHOT (DESIGN.md §14.2) for a kind = 'annotation' row: the fold
-- of the target message's annotations as of that annotation. It lets
-- every node — including ones that never witnessed earlier
-- annotations — deliver the identical summary from the cm itself
-- rather than recomputing. The current summary for message reads
-- lives on the messages projection payload instead; this column is
-- purely the cross-node delivery carrier.

CREATE TABLE channel_messages (
  channel        TEXT    NOT NULL,
  channel_serial TEXT    NOT NULL,
  idx            INT     NOT NULL,
  id             TEXT,
  kind           TEXT    NOT NULL DEFAULT 'message',  -- 'message' | 'presence' (§12.1)
  payload        BYTEA   NOT NULL,
  message_serial TEXT,
  is_append      BOOLEAN NOT NULL DEFAULT FALSE,
  summary        BYTEA,
  PRIMARY KEY (channel, channel_serial, idx)
);

-- Idempotency: a non-NULL client-supplied id must be unique per channel
-- within the retention window (shared across message and presence
-- kinds). The partial index skips NULL ids so publishes without an id
-- never collide.
CREATE UNIQUE INDEX channel_messages_idempotency_idx
  ON channel_messages (channel, id)
  WHERE id IS NOT NULL;

-- The serial→versions index: it backs the by-serial version scan in
-- version order and the update/delete merge-against-target lookup.
CREATE INDEX channel_messages_versions_idx
  ON channel_messages (channel, message_serial, channel_serial, idx)
  WHERE message_serial IS NOT NULL;

-- channels table tracks the current channelSerial for each channel —
-- the cluster-wide source of truth for "the next serial we'll mint."
-- Every Storage.Channel() materialisation upserts a row with an
-- initial serial; every publish atomically advances the row inside
-- the same transaction that inserts the messages.
--
-- Moving serial minting into Postgres (vs the process-local
-- serial.Generator) gives a total order across nodes: previously, two
-- processes at the same wall-clock millisecond could produce serials
-- that lex-ordered out of commit order (disambiguated only by
-- seriesId). With the channels-row lock, every advance reads the last
-- minted serial and produces one strictly greater than it, regardless
-- of which node issued the call.
--
-- initial_channel_serial is the channel's IMMUTABLE initial serial —
-- the seed minted when the channels row was first created. Distinct
-- from channel_serial (which advances on every publish),
-- initial_channel_serial is set on INSERT and never updated, so it
-- always sorts strictly less than every cm ever persisted on this
-- channel. Used by rewind ATTACHED responses as the attach point when
-- the rewind window covers the entire channel (no predecessor cm
-- exists in storage to use instead).

CREATE TABLE channels (
  name                   TEXT NOT NULL PRIMARY KEY,
  channel_serial         TEXT NOT NULL,
  initial_channel_serial TEXT NOT NULL
);

-- format_channel_serial assembles a channelSerial in the canonical
-- `<14-digit-ts>-<3-digit-ctr>@<series>` format used by the
-- internal/serial package. Kept SQL-side so the entire mint is one
-- function call.
CREATE FUNCTION format_channel_serial(p_ts BIGINT, p_ctr INT, p_series TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
  SELECT lpad(p_ts::text, 14, '0') || '-' || lpad(p_ctr::text, 3, '0') || '@' || p_series;
$$;

-- next_channel_serial computes the successor serial for a given
-- previous serial, applying the same monotonicity rules as the
-- in-process Generator: if wall-clock advanced, reset counter; else
-- bump counter (carrying ts forward when the counter overflows 999).
CREATE FUNCTION next_channel_serial(p_prev TEXT, p_series TEXT) RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
  prev_ts  BIGINT;
  prev_ctr INT;
  now_ms   BIGINT := (extract(epoch from clock_timestamp()) * 1000)::BIGINT;
  next_ts  BIGINT;
  next_ctr INT;
BEGIN
  prev_ts  := substr(p_prev, 1, 14)::BIGINT;
  prev_ctr := substr(p_prev, 16, 3)::INT;

  IF now_ms > prev_ts THEN
    next_ts  := now_ms;
    next_ctr := 0;
  ELSIF prev_ctr < 999 THEN
    next_ts  := prev_ts;
    next_ctr := prev_ctr + 1;
  ELSE
    next_ts  := prev_ts + 1;
    next_ctr := 0;
  END IF;

  RETURN format_channel_serial(next_ts, next_ctr, p_series);
END;
$$;

-- ensure_channel returns the current and initial channel_serial for a
-- channel, initialising the row (seeding both columns with the same
-- freshly-minted serial) if it does not yet exist. current_serial is
-- the watermark fresh attaches use as their cursor; initial_serial is
-- the immutable rewind floor described above.
CREATE FUNCTION ensure_channel(p_name TEXT, p_series TEXT)
  RETURNS TABLE(current_serial TEXT, initial_serial TEXT)
LANGUAGE plpgsql AS $$
DECLARE
  seed TEXT;
BEGIN
  seed := format_channel_serial(
    (extract(epoch from clock_timestamp()) * 1000)::BIGINT,
    0,
    p_series
  );
  INSERT INTO channels (name, channel_serial, initial_channel_serial)
    VALUES (p_name, seed, seed)
    ON CONFLICT (name) DO UPDATE SET channel_serial = channels.channel_serial;
  RETURN QUERY
    SELECT channel_serial, initial_channel_serial
    FROM channels
    WHERE name = p_name;
END;
$$;

-- advance_channel_serial returns the next channelSerial for a publish
-- to p_name and atomically writes it back to the channels row. The
-- INSERT … ON CONFLICT path covers the rare case of a publish to a
-- channel whose row hasn't been materialised yet (e.g. a publish
-- arrives before Storage.Channel ran for that name on this node);
-- absent prior state, the function treats it the same as a fresh
-- channel — seeding initial_channel_serial too — and the next publish
-- on it advances from this seed.
CREATE FUNCTION advance_channel_serial(p_name TEXT, p_series TEXT) RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
  fresh TEXT;
  new_serial TEXT;
BEGIN
  fresh := format_channel_serial(
    (extract(epoch from clock_timestamp()) * 1000)::BIGINT,
    0,
    p_series
  );
  INSERT INTO channels (name, channel_serial, initial_channel_serial)
    VALUES (p_name, fresh, fresh)
    ON CONFLICT (name) DO UPDATE
      SET channel_serial = next_channel_serial(channels.channel_serial, p_series)
    RETURNING channel_serial INTO new_serial;
  RETURN new_serial;
END;
$$;

-- Materialised current PRESENCE state: one row per live member, keyed
-- by (connectionId, clientId). ENTER/UPDATE upsert, LEAVE deletes —
-- maintained in the same transaction as the presence cm's INSERT into
-- channel_messages (DESIGN.md §12.5), so the set stays consistent with
-- the log. Held in Postgres (not in-memory like the single-process
-- backends) so it is globally authoritative across cluster nodes.
--
-- node_id and expires_at are the liveness lease used by the
-- crashed-node reaper (DESIGN.md §12.5): a cluster node that dies
-- without running teardown cannot emit its LEAVEs, orphaning rows in
-- this table (a cluster-only problem — a single-process crash takes
-- the whole in-memory set down with it). The owning node bumps
-- expires_at for all of its rows on a cadence shorter than the lease
-- window, and a periodic reaper DELETEs rows whose lease has lapsed,
-- synthesising a LEAVE for each. This bounds orphan visibility to one
-- lease window.
CREATE TABLE presence (
  channel        TEXT        NOT NULL,
  connection_id  TEXT        NOT NULL,
  client_id      TEXT        NOT NULL,
  channel_serial TEXT        NOT NULL,  -- serial of the latest ENTER/UPDATE
  payload        BYTEA       NOT NULL,  -- msgpack-encoded protocol.PresenceMessage
  node_id        TEXT        NOT NULL DEFAULT '',
  expires_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel, connection_id, client_id)
);

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

-- Track each channel's IMMUTABLE initial serial — the seed minted
-- when the channels row was first created. Distinct from
-- channel_serial (which advances on every publish), initial_channel_serial
-- is set on INSERT and never updated, so it always sorts strictly less
-- than every cm ever persisted on this channel.
--
-- Used by rewind ATTACHED responses as the attach point when the
-- rewind window covers the entire channel (no predecessor cm exists
-- in storage to use instead).

ALTER TABLE channels ADD COLUMN initial_channel_serial TEXT;

-- Backfill for any rows that pre-date this migration: use the current
-- channel_serial as a best-effort initial. New rows after this
-- migration use the seed value (set in the updated ensure_channel /
-- advance_channel_serial below).
UPDATE channels SET initial_channel_serial = channel_serial
  WHERE initial_channel_serial IS NULL;

ALTER TABLE channels ALTER COLUMN initial_channel_serial SET NOT NULL;

-- Recreate ensure_channel to return both the current and initial
-- serials, and to set initial_channel_serial = the seed on first
-- INSERT.
DROP FUNCTION ensure_channel(TEXT, TEXT);

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

-- Update advance_channel_serial to set initial_channel_serial when it
-- creates a fresh row (the rare case of a publish landing before
-- Storage.Channel has materialised the channel on any node).
CREATE OR REPLACE FUNCTION advance_channel_serial(p_name TEXT, p_series TEXT) RETURNS TEXT
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

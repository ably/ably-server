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

CREATE TABLE channels (
  name           TEXT NOT NULL PRIMARY KEY,
  channel_serial TEXT NOT NULL
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

-- ensure_channel returns the current channel_serial for a channel,
-- initialising the row with a freshly-minted serial if it does not
-- yet exist. The returned serial is the watermark fresh attaches use
-- as their cursor.
CREATE FUNCTION ensure_channel(p_name TEXT, p_series TEXT) RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
  initial TEXT;
  current TEXT;
BEGIN
  initial := format_channel_serial(
    (extract(epoch from clock_timestamp()) * 1000)::BIGINT,
    0,
    p_series
  );
  INSERT INTO channels (name, channel_serial) VALUES (p_name, initial)
    ON CONFLICT (name) DO UPDATE SET channel_serial = channels.channel_serial
    RETURNING channel_serial INTO current;
  RETURN current;
END;
$$;

-- advance_channel_serial returns the next channelSerial for a publish
-- to p_name and atomically writes it back to the channels row. The
-- INSERT … ON CONFLICT path covers the rare case of a publish to a
-- channel whose row hasn't been materialised yet (e.g. a publish
-- arrives before Storage.Channel ran for that name on this node);
-- absent prior state, the function treats it the same as a fresh
-- channel and the next publish on it advances from this seed.
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
  INSERT INTO channels (name, channel_serial) VALUES (p_name, fresh)
    ON CONFLICT (name) DO UPDATE
      SET channel_serial = next_channel_serial(channels.channel_serial, p_series)
    RETURNING channel_serial INTO new_serial;
  RETURN new_serial;
END;
$$;

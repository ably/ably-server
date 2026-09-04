-- The seriesId belongs to the channel, not to the node that happens to
-- be publishing.
--
-- 0001 minted every serial with the calling process's seriesId, so in
-- cluster mode a channel's serials changed series whenever the
-- publishing node changed. A change of series is not cosmetic: it is
-- how a server tells a client that serials before it are not ordered
-- against the ones after it, so subscribers saw an epoch change on
-- every handover, on a channel whose log was entirely intact.
--
-- The series 0001 was carrying was never needed here anyway. It exists
-- to disambiguate serials minted in the same millisecond by different
-- processes, and the channels-row mint removed that possibility when it
-- replaced the process-local generator: every advance reads the last
-- minted serial under the row lock and produces one strictly greater
-- than it, whichever node asked.
--
-- No backfill and no new column: the channel's series is already in the
-- channels row, inside channel_serial. What changes is that a mint now
-- reads it from there instead of substituting the caller's, so p_series
-- survives only where it is genuinely needed — seeding a channel that
-- does not exist yet.

DROP FUNCTION next_channel_serial(TEXT, TEXT);

-- next_channel_serial computes the successor serial for a given
-- previous serial, applying the same monotonicity rules as the
-- in-process Generator: if wall-clock advanced, reset counter; else
-- bump counter (carrying ts forward when the counter overflows 999).
-- The series is the previous serial's, so a channel keeps the one it
-- was seeded with for as long as its row lives.
CREATE FUNCTION next_channel_serial(p_prev TEXT) RETURNS TEXT
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

  RETURN format_channel_serial(next_ts, next_ctr, split_part(p_prev, '@', 2));
END;
$$;

-- advance_channel_serial returns the next channelSerial for a publish
-- to p_name and atomically writes it back to the channels row. p_series
-- seeds a channel whose row has not been materialised yet (a publish
-- that arrives before Storage.Channel ran for that name on this node);
-- an existing row advances from its own serial, series included, so the
-- caller's series is not consulted.
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
      SET channel_serial = next_channel_serial(channels.channel_serial)
    RETURNING channel_serial INTO new_serial;
  RETURN new_serial;
END;
$$;

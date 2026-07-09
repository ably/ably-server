-- Presence liveness lease + crashed-node reaper (DESIGN.md §12.5).
--
-- A cluster node that dies without running teardown cannot emit its
-- LEAVEs, orphaning rows in the presence table (a cluster-only problem:
-- a single-process crash takes the whole in-memory set down with it).
-- Each row now records its owning node_id and an expires_at lease: the
-- owning node bumps expires_at for all of its rows on a cadence shorter
-- than the lease window, and a periodic reaper DELETEs rows whose lease
-- has lapsed, synthesising a LEAVE for each. This bounds orphan
-- visibility to one lease window.
--
-- Forward-only. The columns carry defaults so the ADD COLUMN succeeds
-- against a table that may already hold rows; StorePresence always
-- writes both explicitly, and any pre-existing orphan (node_id = '')
-- is reaped on the next sweep.

ALTER TABLE presence
  ADD COLUMN node_id    TEXT        NOT NULL DEFAULT '',
  ADD COLUMN expires_at TIMESTAMPTZ NOT NULL DEFAULT now();

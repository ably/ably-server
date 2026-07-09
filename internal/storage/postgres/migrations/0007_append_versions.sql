-- Streamed appends (DESIGN.md §13.3). An append is persisted on the log
-- as a full action=update version carrying the rolled-up aggregate, so
-- the log stays append-only and live/resume fan-out sees every append.
-- The version-history read-path, however, must collapse a run of appends
-- to its aggregate rather than enumerate each delta. is_append marks the
-- rows that are appends so the versions query (a LEAD window over the
-- log) can drop every append but the last of each run.
ALTER TABLE channel_messages ADD COLUMN is_append BOOLEAN NOT NULL DEFAULT FALSE;

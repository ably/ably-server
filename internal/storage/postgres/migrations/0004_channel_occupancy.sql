-- Per-node channel occupancy (DESIGN.md §16.2): one row per (channel,
-- node) holding what that node currently contributes — how many holders
-- it serves and in which modes.
--
-- A channel's occupancy is a single, node-independent thing — nine
-- subscribers is nine subscribers, wherever they are connected. The rows
-- are per-node because a count has no identity: a bare global total that
-- nodes incremented and decremented could not survive one of them dying,
-- since there would be no way to subtract a share that was never recorded
-- as anyone's. Attributing each share to its owner is what lets the
-- reaper remove exactly what a dead node contributed.
--
-- That is the difference from presence, whose members are globally
-- identified (connectionId:clientId) and so can be picked out and removed
-- one by one without anyone recording a total.
--
-- The consequence is that the aggregate is a SUM over this table rather
-- than a read of a single materialised row, and no row here is
-- authoritative on its own.
--
-- The counts are columns rather than an encoded blob so the sum happens
-- in SQL: a node reading the aggregate should not have to decode every
-- other node's contribution to add it up. channel_mode is bit_or'd
-- rather than summed — a mode is occupied if any node has a holder of
-- it.
--
-- presenceMembers is deliberately absent. It is not a per-node
-- contribution: the membership set is already global (§12.5), so summing
-- it here would multiply it by the number of nodes holding the channel.
-- The aggregate reads it from the presence table instead.
--
-- node_id and expires_at are the same liveness lease presence uses
-- (§12.5). A node that dies without running teardown cannot zero its own
-- contribution, which would leave the channel looking permanently
-- occupied by connections that no longer exist — a worse failure than
-- the presence equivalent, since nothing else would ever correct it. The
-- owning node bumps expires_at on a cadence shorter than the lease
-- window and the reaper deletes rows whose lease has lapsed, bounding
-- the overcount to one lease window.
CREATE TABLE channel_occupancy (
  channel              TEXT        NOT NULL,
  node_id              TEXT        NOT NULL,
  channel_mode         INT         NOT NULL DEFAULT 0,
  connections          INT         NOT NULL DEFAULT 0,
  publishers           INT         NOT NULL DEFAULT 0,
  subscribers          INT         NOT NULL DEFAULT 0,
  presence_connections INT         NOT NULL DEFAULT 0,
  presence_subscribers INT         NOT NULL DEFAULT 0,
  object_subscribers   INT         NOT NULL DEFAULT 0,
  object_publishers    INT         NOT NULL DEFAULT 0,
  expires_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel, node_id)
);

-- The reaper sweeps by lease across every channel, so it wants the lease
-- ordered independently of the channel it belongs to.
CREATE INDEX channel_occupancy_expiry_idx ON channel_occupancy (expires_at);

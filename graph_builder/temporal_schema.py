"""Stage 2b temporal tables: events (changelog mirror) + attr_history (folded
valid-time intervals). Additive to Stage 2a's nodes/edges — init_temporal drops
& recreates ONLY these two tables.
"""
from __future__ import annotations

import sqlite3

TEMPORAL_SCHEMA_SQL = """
DROP TABLE IF EXISTS attr_history;
DROP TABLE IF EXISTS events;

CREATE TABLE events (
  event_id  INTEGER PRIMARY KEY,
  ticket_id TEXT,
  ts        TEXT,
  author    TEXT,
  field     TEXT,
  from_id   TEXT,
  from_val  TEXT,
  to_id     TEXT,
  to_val    TEXT
);

CREATE TABLE attr_history (
  node_id    TEXT,
  attr       TEXT,
  value      TEXT,
  valid_from TEXT,
  valid_to   TEXT NOT NULL DEFAULT '9999-12-31',
  source     TEXT,
  PRIMARY KEY (node_id, attr, valid_from)
) WITHOUT ROWID;
"""

TEMPORAL_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_events_ticket_field_ts ON events(ticket_id, field, ts);
CREATE INDEX IF NOT EXISTS ix_events_ts               ON events(ts);
CREATE INDEX IF NOT EXISTS ix_attr_asof               ON attr_history(node_id, attr, valid_from, valid_to);
"""


def init_temporal(conn: sqlite3.Connection) -> None:
    conn.executescript(TEMPORAL_SCHEMA_SQL)
    conn.commit()


def create_temporal_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(TEMPORAL_INDEX_SQL)
    conn.commit()

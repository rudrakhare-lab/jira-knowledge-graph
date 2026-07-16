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


ALIAS_LINK_SCHEMA_SQL = """
DROP TABLE IF EXISTS key_alias;
DROP TABLE IF EXISTS link_events;

CREATE TABLE key_alias (
  old_key     TEXT PRIMARY KEY,
  current_key TEXT
);

CREATE TABLE link_events (
  link_event_id INTEGER PRIMARY KEY,
  ticket_id     TEXT,
  ts            TEXT,
  action        TEXT,
  target_key    TEXT,
  type_phrase   TEXT,
  mapped_type   TEXT
);
"""

ALIAS_LINK_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_link_events_ticket_ts ON link_events(ticket_id, ts);
CREATE INDEX IF NOT EXISTS ix_link_events_target    ON link_events(target_key);
"""


def init_alias_link(conn: sqlite3.Connection) -> None:
    conn.executescript(ALIAS_LINK_SCHEMA_SQL)
    conn.commit()


def create_alias_link_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(ALIAS_LINK_INDEX_SQL)
    conn.commit()

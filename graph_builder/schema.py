"""SQLite schema for the Stage-2a knowledge graph (nodes + edges).

Temporal columns exist now with creation-time defaults; Stage 2b (changelog
fold) refines link validity without a schema change.
"""
from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS nodes;

CREATE TABLE nodes (
  id          TEXT PRIMARY KEY,
  type        TEXT NOT NULL,
  attrs       TEXT,                 -- JSON, immutable/descriptive only
  created_ts  TEXT,                 -- ticket creation; NULL for non-ticket nodes
  recorded_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE edges (
  src             TEXT NOT NULL,
  dst             TEXT NOT NULL,
  type            TEXT NOT NULL,
  attrs           TEXT,
  link_id         TEXT,             -- Jira issuelink id (provenance; NULL for non-link edges)
  valid_from      TEXT,             -- creation-time default in 2a; refined in 2b
  valid_to        TEXT NOT NULL DEFAULT '9999-12-31',
  type_confidence TEXT DEFAULT 'exact',
  recorded_at     TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (src, dst, type)
) WITHOUT ROWID;
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_edges_src_asof ON edges(src, type, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS ix_edges_dst_asof ON edges(dst, type, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS ix_nodes_type      ON nodes(type);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    # bulk-load pragmas (safe: v1 rebuilds from scratch on any failure)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.executescript(SCHEMA_SQL)
    return conn


def create_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(INDEX_SQL)
    conn.commit()

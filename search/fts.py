"""Stage 3 (part 1) — FTS5 keyword search over ticket text.

`tickets_fts` is an FTS5 virtual table in the shared graph.db; init_fts drops &
recreates ONLY it (its shadow tables go with it). BM25 ranking via bm25().
"""
from __future__ import annotations

import re
import sqlite3

from graph_builder.adf import adf_to_text

FTS_SCHEMA_SQL = """
DROP TABLE IF EXISTS tickets_fts;
CREATE VIRTUAL TABLE tickets_fts USING fts5(
  key UNINDEXED,
  summary,
  description,
  comments
);
"""


def init_fts(conn: sqlite3.Connection) -> None:
    conn.executescript(FTS_SCHEMA_SQL)
    conn.commit()


def searchable_text(record: dict) -> dict:
    f = record.get("fields") or {}
    summary = f.get("summary") or ""
    description = adf_to_text(f.get("description"))
    parts = []
    for c in (record.get("comments") or []):
        parts.append(adf_to_text(c.get("body")))
    return {"key": record["key"], "summary": summary,
            "description": description, "comments": " ".join(parts)}

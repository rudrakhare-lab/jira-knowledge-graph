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


def _fts_query(raw: str) -> str:
    r"""Tokenize raw text to word-character terms, double-quote each, join with spaces.

    Prevents FTS5 syntax injection by extracting only \w+ terms (alphanumeric + underscore),
    lowercasing, and wrapping in quotes. Empty/no-terms returns empty string.
    """
    terms = re.findall(r"\w+", (raw or "").lower())
    return " ".join('"%s"' % t for t in terms)


def search_bm25(conn: sqlite3.Connection, query: str, limit: int = 20):
    """Search tickets_fts via BM25 ranking.

    Returns list of (key, score) tuples ranked best-first (more negative score = better).
    Empty sanitized query returns [].
    """
    q = _fts_query(query)
    if not q:
        return []
    return conn.execute(
        "SELECT key, bm25(tickets_fts) AS score FROM tickets_fts "
        "WHERE tickets_fts MATCH ? ORDER BY score LIMIT ?", (q, limit)).fetchall()

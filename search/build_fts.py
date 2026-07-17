"""Stage 3 (part 1) driver: stream tickets.jsonl -> tickets_fts in the EXISTING
graph.db. Never touches other tables. Constant memory; re-run rebuilds only
tickets_fts.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

from search.fts import init_fts, searchable_text

logger = logging.getLogger("search.build_fts")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def _flush(conn, rows):
    if rows:
        conn.executemany(
            "INSERT INTO tickets_fts(key,summary,description,comments) "
            "VALUES(:key,:summary,:description,:comments)", rows)


def build_fts(jsonl_path: str, db_path: str, batch_size: int = 2000,
              limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    # WAL + synchronous=NORMAL: durable (crash-safe) yet fast on the shared graph.db;
    # graph.db is already WAL from Stage 2a. journal_mode persists in the db header.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_fts(conn)

    records = indexed = 0
    rows: list[dict] = []
    with open(jsonl_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            records += 1
            if not rec.get("key"):
                continue
            rows.append(searchable_text(rec))
            indexed += 1
            if len(rows) >= batch_size:
                _flush(conn, rows); rows.clear()
            if records % 50000 == 0:
                conn.commit(); logger.info("indexed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, rows)
    conn.commit()
    counts = {"records": records, "indexed": indexed}
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    logger.info("done: %s", build_fts(jsonl, db, limit=limit))


if __name__ == "__main__":
    main()

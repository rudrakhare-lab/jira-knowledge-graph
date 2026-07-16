"""Stage 2b-part-3 driver: stream tickets.jsonl -> link_history in the EXISTING
graph.db. Never touches other tables. Constant memory; re-run rebuilds only
link_history.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

from graph_builder import temporal_schema
from graph_builder.replay import fold_link_history

logger = logging.getLogger("graph_builder.build_link_history")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def _flush(conn, rows):
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO link_history"
            "(node_id,target_key,link_type,valid_from,valid_to,source) "
            "VALUES(:node_id,:target_key,:link_type,:valid_from,:valid_to,:source)", rows)


def build_link_history(jsonl_path: str, db_path: str, batch_size: int = 5000,
                       limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    temporal_schema.init_link_history(conn)

    records = interval_count = disc_count = 0
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
            lh, disc = fold_link_history(rec)
            rows.extend(lh)
            interval_count += len(lh); disc_count += len(disc)
            if len(rows) >= batch_size:
                _flush(conn, rows); rows.clear()
            if records % 50000 == 0:
                conn.commit(); logger.info("processed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, rows)
    conn.commit()
    temporal_schema.create_link_history_indexes(conn)
    counts = {"records": records, "intervals": interval_count, "discrepancies": disc_count}
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    logger.info("done: %s", build_link_history(jsonl, db, limit=limit))


if __name__ == "__main__":
    main()

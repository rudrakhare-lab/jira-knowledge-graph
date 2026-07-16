"""Stage 2b driver: stream tickets.jsonl -> events + attr_history in the EXISTING
graph.db. Never touches nodes/edges. Constant memory; re-run rebuilds temporal
tables only.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

from graph_builder import temporal_schema
from graph_builder.replay import extract_events, fold_attr_history

logger = logging.getLogger("graph_builder.build_temporal")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def _flush(conn, ev_rows, ah_rows):
    if ev_rows:
        conn.executemany(
            "INSERT INTO events(ticket_id,ts,author,field,from_id,from_val,to_id,to_val) "
            "VALUES(:ticket_id,:ts,:author,:field,:from_id,:from_val,:to_id,:to_val)", ev_rows)
    if ah_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO attr_history(node_id,attr,value,valid_from,valid_to,source) "
            "VALUES(:node_id,:attr,:value,:valid_from,:valid_to,:source)", ah_rows)


def build_temporal(jsonl_path: str, db_path: str, batch_size: int = 5000,
                   limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    temporal_schema.init_temporal(conn)

    records = ev_count = ah_count = disc_count = 0
    ev_rows: list[dict] = []
    ah_rows: list[dict] = []
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
            evs = extract_events(rec)
            ah, disc = fold_attr_history(rec)
            ev_rows.extend(evs); ah_rows.extend(ah)
            ev_count += len(evs); ah_count += len(ah); disc_count += len(disc)
            if len(ev_rows) >= batch_size or len(ah_rows) >= batch_size:
                _flush(conn, ev_rows, ah_rows); ev_rows.clear(); ah_rows.clear()
            if records % 50000 == 0:
                conn.commit()
                logger.info("processed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, ev_rows, ah_rows)
    conn.commit()
    temporal_schema.create_temporal_indexes(conn)
    counts = {"records": records, "events": ev_count,
              "attr_rows": ah_count, "discrepancies": disc_count}
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    logger.info("done: %s", build_temporal(jsonl, db, limit=limit))


if __name__ == "__main__":
    main()

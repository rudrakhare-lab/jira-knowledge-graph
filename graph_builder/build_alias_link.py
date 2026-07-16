"""Stage 2b-part-2 driver: stream tickets.jsonl -> key_alias + link_events in the
EXISTING graph.db. Never touches nodes/edges/events/attr_history. Constant memory;
re-run rebuilds only these two tables.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

from graph_builder import temporal_schema
from graph_builder.replay import extract_key_aliases, extract_link_events

logger = logging.getLogger("graph_builder.build_alias_link")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def resolve_key(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute(
        "SELECT current_key FROM key_alias WHERE old_key = ?", (key,)).fetchone()
    return row[0] if row else key


def _flush(conn, alias_rows, link_rows):
    if alias_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO key_alias(old_key,current_key) "
            "VALUES(:old_key,:current_key)", alias_rows)
    if link_rows:
        conn.executemany(
            "INSERT INTO link_events(ticket_id,ts,action,target_key,type_phrase,mapped_type) "
            "VALUES(:ticket_id,:ts,:action,:target_key,:type_phrase,:mapped_type)", link_rows)


def build_alias_link(jsonl_path: str, db_path: str, batch_size: int = 5000,
                     limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    temporal_schema.init_alias_link(conn)

    records = alias_count = link_count = 0
    alias_rows: list[dict] = []
    link_rows: list[dict] = []
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
            al = extract_key_aliases(rec)
            le = extract_link_events(rec)
            alias_rows.extend(al); link_rows.extend(le)
            alias_count += len(al); link_count += len(le)
            if len(alias_rows) >= batch_size or len(link_rows) >= batch_size:
                _flush(conn, alias_rows, link_rows)
                alias_rows.clear(); link_rows.clear()
            if records % 50000 == 0:
                conn.commit(); logger.info("processed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, alias_rows, link_rows)
    conn.commit()
    temporal_schema.create_alias_link_indexes(conn)
    counts = {"records": records, "aliases": alias_count, "link_events": link_count}
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    logger.info("done: %s", build_alias_link(jsonl, db, limit=limit))


if __name__ == "__main__":
    main()

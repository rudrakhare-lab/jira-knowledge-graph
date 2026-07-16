"""Stage 2a driver: stream tickets.jsonl -> SQLite graph (nodes + edges).

Constant memory: read line-by-line, batch INSERT OR IGNORE, build indexes
after bulk load. Deterministic; re-running overwrites (init_db drops tables).
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from graph_builder import schema
from graph_builder.extract import extract_all

logger = logging.getLogger("graph_builder.build")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "tickets.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "graph.db"


def _flush(conn, node_rows, edge_rows):
    if node_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO nodes(id,type,attrs,created_ts) VALUES(?,?,?,?)",
            node_rows)
    if edge_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO edges(src,dst,type,valid_from,type_confidence,link_id) "
            "VALUES(?,?,?,?,?,?)",
            edge_rows)


def build(jsonl_path: str, db_path: str, batch_size: int = 5000,
          limit: int | None = None) -> dict:
    conn = schema.init_db(db_path)
    records = 0
    node_rows: list[tuple] = []
    edge_rows: list[tuple] = []
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
            nodes, edges = extract_all(rec)
            for n in nodes:
                node_rows.append((n["id"], n["type"],
                                  json.dumps(n["attrs"], ensure_ascii=False),
                                  n["created_ts"]))
            for e in edges:
                edge_rows.append((e["src"], e["dst"], e["type"], e["valid_from"],
                                  e["type_confidence"], e["link_id"]))
            if len(node_rows) >= batch_size or len(edge_rows) >= batch_size:
                _flush(conn, node_rows, edge_rows)
                node_rows.clear()
                edge_rows.clear()
            if records % 50000 == 0:
                conn.commit()
                logger.info("processed %d records", records)
            if limit and records >= limit:
                break
    _flush(conn, node_rows, edge_rows)
    conn.commit()
    schema.create_indexes(conn)
    counts = {
        "records": records,
        "nodes": conn.execute("SELECT count(*) FROM nodes").fetchone()[0],
        "edges": conn.execute("SELECT count(*) FROM edges").fetchone()[0],
    }
    conn.close()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    jsonl = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_JSONL)
    db = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DB)
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    counts = build(jsonl, db, limit=limit)
    logger.info("done: %s", counts)


if __name__ == "__main__":
    main()

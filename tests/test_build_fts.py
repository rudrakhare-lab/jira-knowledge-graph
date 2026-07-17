import sqlite3
from pathlib import Path

from search import build_fts, fts

FIX = Path(__file__).parent / "fixtures" / "fts_tickets.jsonl"


def test_build_fts_and_search(tmp_path):
    db = str(tmp_path / "g.db")
    c0 = sqlite3.connect(db)
    c0.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    c0.execute("INSERT INTO nodes VALUES ('SUP-1','Ticket')")
    c0.commit(); c0.close()

    counts = build_fts.build_fts(str(FIX), db, batch_size=2)
    assert counts == {"records": 3, "indexed": 3}

    conn = sqlite3.connect(db)
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1  # untouched
    # known-item: "504 gateway timeout" -> SUP-1 ranks first
    res = fts.search_bm25(conn, "gateway timeout", limit=5)
    assert res[0][0] == "SUP-1"
    # comment-only term still matches (SUP-2 has 'timeout' only in a comment)
    keys = {k for k, _ in fts.search_bm25(conn, "timeout", limit=5)}
    assert {"SUP-1", "SUP-2"} <= keys and "SUP-3" not in keys
    conn.close()


def test_build_fts_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build_fts.build_fts(str(FIX), db, batch_size=2)
    b = build_fts.build_fts(str(FIX), db, batch_size=2)
    assert a == b


def test_build_fts_skips_records_without_key(tmp_path):
    p = tmp_path / "mixed.jsonl"
    p.write_text(
        '{"key":"K-1","fields":{"summary":"has key"},"comments":[]}\n'
        '{"fields":{"summary":"no key here"},"comments":[]}\n')
    counts = build_fts.build_fts(str(p), str(tmp_path / "g.db"), batch_size=5)
    assert counts == {"records": 2, "indexed": 1}

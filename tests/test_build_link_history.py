import sqlite3
from pathlib import Path

from graph_builder import build_link_history

FIX = Path(__file__).parent / "fixtures" / "link_history_tickets.jsonl"


def test_build_link_history(tmp_path):
    db = str(tmp_path / "g.db")
    c0 = sqlite3.connect(db)
    c0.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    c0.execute("INSERT INTO nodes VALUES ('EIMV2-14875','Ticket')")
    c0.commit(); c0.close()

    counts = build_link_history.build_link_history(str(FIX), db, batch_size=2)
    assert counts["records"] == 2
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1  # untouched

    # EIM-15015: added 10-24, removed 11-24 -> closed interval
    r1 = conn.execute("SELECT valid_from,valid_to,source FROM link_history "
                      "WHERE node_id='EIMV2-14875' AND target_key='EIM-15015'").fetchall()
    assert r1 == [("2014-10-24T00:00:00.000+0530","2014-11-24T00:00:00.000+0530","changelog")]

    # EIM-15379: added 11-26 and present in snapshot -> open interval
    r2 = conn.execute("SELECT valid_from,valid_to FROM link_history "
                      "WHERE node_id='EIMV2-14875' AND target_key='EIM-15379'").fetchall()
    assert r2 == [("2014-11-26T00:00:00.000+0530","9999-12-31")]

    # as-of 2014-11-01: EIM-15015 active, EIM-15379 not yet
    d = "2014-11-01T00:00:00.000+0530"
    active = {r[0] for r in conn.execute(
        "SELECT target_key FROM link_history WHERE node_id='EIMV2-14875' "
        "AND valid_from<=? AND valid_to>?", (d, d))}
    assert active == {"EIM-15015"}
    conn.close()


def test_build_link_history_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build_link_history.build_link_history(str(FIX), db, batch_size=2)
    b = build_link_history.build_link_history(str(FIX), db, batch_size=2)
    assert a == b

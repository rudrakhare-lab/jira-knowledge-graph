import sqlite3
from pathlib import Path

from graph_builder import build_temporal

FIX = Path(__file__).parent / "fixtures" / "temporal_tickets.jsonl"


def test_build_temporal_populates_events_and_history(tmp_path):
    db = str(tmp_path / "g.db")
    # pre-existing Stage-2a table must survive
    c0 = sqlite3.connect(db)
    c0.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    c0.execute("INSERT INTO nodes VALUES ('T-1','Ticket')")
    c0.commit(); c0.close()

    counts = build_temporal.build_temporal(str(FIX), db, batch_size=2)
    assert counts["records"] == 3
    assert counts["events"] == 3          # T-1 status, T-3 two assignee
    conn = sqlite3.connect(db)
    # nodes untouched
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1

    # T-1 status: seeded Open then Done (2 intervals)
    t1 = conn.execute(
        "SELECT value,valid_from,valid_to,source FROM attr_history "
        "WHERE node_id='T-1' AND attr='status' ORDER BY valid_from").fetchall()
    assert t1 == [("Open","2020-01-01T00:00:00.000+0000","2020-02-01T00:00:00.000+0000","changelog"),
                  ("Done","2020-02-01T00:00:00.000+0000","9999-12-31","changelog")]

    # T-2 zero-event fields -> snapshot-seed single intervals
    t2p = conn.execute(
        "SELECT value,source FROM attr_history WHERE node_id='T-2' AND attr='priority'").fetchall()
    assert t2p == [("Minor","snapshot-seed")]

    # as-of query: who was assigned to T-3 on 2019-05-01T12:00? -> acc-b
    asof = conn.execute(
        "SELECT value FROM attr_history WHERE node_id='T-3' AND attr='assignee' "
        "AND valid_from <= ? AND valid_to > ?",
        ("2019-05-01T12:00:00.000+0000","2019-05-01T12:00:00.000+0000")).fetchone()
    assert asof[0] == "acc-b"
    conn.close()


def test_build_temporal_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build_temporal.build_temporal(str(FIX), db, batch_size=2)
    b = build_temporal.build_temporal(str(FIX), db, batch_size=2)
    assert a == b

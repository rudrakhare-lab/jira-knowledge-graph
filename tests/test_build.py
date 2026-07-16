from pathlib import Path

from graph_builder import build

FIX = Path(__file__).parent / "fixtures" / "sample_tickets.jsonl"


def test_build_counts_and_dedup(tmp_path):
    db = str(tmp_path / "g.db")
    counts = build.build(str(FIX), db, batch_size=2)
    assert counts["records"] == 4
    import sqlite3
    conn = sqlite3.connect(db)
    # 4 ticket nodes + entities (project SUP, PB; users acc-r/acc-a/... ; component, labels, sprint, version)
    n_tickets = conn.execute(
        "SELECT count(*) FROM nodes WHERE type IN ('Ticket','Epic')").fetchone()[0]
    assert n_tickets == 4
    # the mirror BLOCKS link (present on SUP-500 outward AND SUP-999 inward) dedups to ONE row
    blocks = conn.execute(
        "SELECT count(*) FROM edges WHERE type='BLOCKS'").fetchone()[0]
    assert blocks == 1
    assert conn.execute(
        "SELECT count(*) FROM edges WHERE src='SUP-500' AND dst='SUP-999' AND type='BLOCKS'"
    ).fetchone()[0] == 1
    # indexes exist (created after load)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_edges_src_asof" in idx
    conn.close()


def test_build_is_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build.build(str(FIX), db, batch_size=2)
    b = build.build(str(FIX), db, batch_size=2)   # re-run overwrites
    assert a == b

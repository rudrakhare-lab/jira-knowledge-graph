import sqlite3
from pathlib import Path

from graph_builder import build_alias_link

FIX = Path(__file__).parent / "fixtures" / "alias_link_tickets.jsonl"


def test_build_alias_link(tmp_path):
    db = str(tmp_path / "g.db")
    c0 = sqlite3.connect(db)
    c0.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    c0.execute("INSERT INTO nodes VALUES ('EIMV2-14875','Ticket')")
    c0.commit(); c0.close()

    counts = build_alias_link.build_alias_link(str(FIX), db, batch_size=2)
    assert counts["records"] == 2
    assert counts["aliases"] == 1
    assert counts["link_events"] == 3      # EIMV2: add+remove; SUP-2: add

    conn = sqlite3.connect(db)
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1  # untouched
    # alias resolves old key -> current
    assert build_alias_link.resolve_key(conn, "EIM-5998") == "EIMV2-14875"
    assert build_alias_link.resolve_key(conn, "SUP-2") == "SUP-2"     # unaliased passthrough
    # link event mapped type
    row = conn.execute("SELECT action,target_key,mapped_type FROM link_events "
                       "WHERE ticket_id='SUP-2'").fetchone()
    assert row == ("add", "SUP-9", "BLOCKS")
    conn.close()


def test_build_alias_link_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    a = build_alias_link.build_alias_link(str(FIX), db, batch_size=2)
    b = build_alias_link.build_alias_link(str(FIX), db, batch_size=2)
    assert a == b

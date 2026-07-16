import sqlite3
from graph_builder import temporal_schema


def test_init_temporal_creates_only_temporal_tables(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    # simulate a Stage-2a db with a nodes table holding a row
    conn.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    conn.execute("INSERT INTO nodes VALUES ('SUP-1','Ticket')")
    conn.commit()
    temporal_schema.init_temporal(conn)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"events", "attr_history", "nodes"} <= tables
    # nodes was NOT dropped
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    ecols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
    acols = {r[1] for r in conn.execute("PRAGMA table_info(attr_history)")}
    assert {"event_id","ticket_id","ts","author","field",
            "from_id","from_val","to_id","to_val"} <= ecols
    assert {"node_id","attr","value","valid_from","valid_to","source"} <= acols
    # idempotent: re-init drops & recreates temporal tables only
    conn.execute("INSERT INTO events(ticket_id,ts,field) VALUES('X','t','status')")
    conn.commit()
    temporal_schema.init_temporal(conn)
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    conn.close()


def test_create_temporal_indexes(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    temporal_schema.init_temporal(conn)
    temporal_schema.create_temporal_indexes(conn)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_events_ticket_field_ts" in idx
    assert "ix_attr_asof" in idx
    conn.close()

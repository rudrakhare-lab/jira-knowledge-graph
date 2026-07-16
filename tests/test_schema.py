import sqlite3
from graph_builder import schema


def test_init_db_creates_tables_and_is_idempotent(tmp_path):
    db = str(tmp_path / "g.db")
    conn = schema.init_db(db)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"nodes", "edges"} <= tables
    # columns present
    node_cols = {r[1] for r in conn.execute("PRAGMA table_info(nodes)")}
    edge_cols = {r[1] for r in conn.execute("PRAGMA table_info(edges)")}
    assert {"id", "type", "attrs", "created_ts"} <= node_cols
    assert {"src", "dst", "type", "valid_from", "valid_to",
            "type_confidence", "link_id"} <= edge_cols
    conn.close()
    # re-init drops & recreates without error (overwrite semantics)
    conn2 = schema.init_db(db)
    assert conn2.execute("SELECT count(*) FROM nodes").fetchone()[0] == 0
    conn2.close()


def test_create_indexes(tmp_path):
    conn = schema.init_db(str(tmp_path / "g.db"))
    schema.create_indexes(conn)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_edges_src_asof" in idx
    assert "ix_edges_dst_asof" in idx
    conn.close()

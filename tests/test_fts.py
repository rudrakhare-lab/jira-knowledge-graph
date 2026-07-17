import sqlite3
from search import fts


def test_init_fts_creates_only_tickets_fts(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    conn.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, type TEXT)")
    conn.execute("INSERT INTO nodes VALUES ('SUP-1','Ticket')")
    conn.commit()
    fts.init_fts(conn)
    # tickets_fts exists and is queryable
    conn.execute("INSERT INTO tickets_fts(key,summary,description,comments) "
                 "VALUES('SUP-1','login timeout','','')")
    conn.commit()
    assert conn.execute("SELECT key FROM tickets_fts WHERE tickets_fts MATCH 'login'"
                        ).fetchone()[0] == "SUP-1"
    # nodes untouched
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    # idempotent: re-init clears tickets_fts, leaves nodes
    fts.init_fts(conn)
    assert conn.execute("SELECT count(*) FROM tickets_fts").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM nodes").fetchone()[0] == 1
    conn.close()

def test_searchable_text_flattens_adf_and_comments():
    rec = {"key": "SUP-5", "fields": {
        "summary": "Login timeout on mobile",
        "description": {"type": "doc", "content": [
            {"type": "paragraph", "content": [
                {"type": "text", "text": "Users hit a 504 after 30s"}]}]}},
        "comments": [
            {"body": {"type": "doc", "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "reproduced on staging"}]}]}}],
        "attachments": [{"extracted_text": "SHOULD NOT BE INDEXED"}]}
    t = fts.searchable_text(rec)
    assert t["key"] == "SUP-5"
    assert t["summary"] == "Login timeout on mobile"
    assert "504" in t["description"]
    assert "reproduced on staging" in t["comments"]
    # attachment text is excluded
    assert "SHOULD NOT BE INDEXED" not in (t["summary"]+t["description"]+t["comments"])


def test_searchable_text_handles_missing_fields():
    t = fts.searchable_text({"key": "X-1", "fields": {}})
    assert t == {"key": "X-1", "summary": "", "description": "", "comments": ""}


def test_fts_query_sanitizes():
    assert fts._fts_query("login timeout") == '"login" "timeout"'
    # hyphen / punctuation split into safe terms (no FTS5 syntax injection)
    assert fts._fts_query("EIM-15015") == '"eim" "15015"'
    assert fts._fts_query("   ") == ""
    assert fts._fts_query('"; DROP') == '"drop"'


def _mk_fts(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "g.db"))
    fts.init_fts(conn)
    rows = [
        ("A-1", "login timeout on mobile", "user sees 504 gateway timeout", ""),
        ("A-2", "payment processing error", "card declined at checkout", "timeout reported once"),
        ("A-3", "dashboard slow", "charts take 10s to render", ""),
    ]
    conn.executemany("INSERT INTO tickets_fts(key,summary,description,comments) "
                     "VALUES(?,?,?,?)", rows)
    conn.commit()
    return conn


def test_search_bm25_ranks_and_filters(tmp_path):
    conn = _mk_fts(tmp_path)
    res = fts.search_bm25(conn, "timeout", limit=10)
    keys = [k for k, _ in res]
    # A-1 (timeout in summary+desc) and A-2 (timeout in comments) match; A-3 does not
    assert set(keys) == {"A-1", "A-2"}
    assert keys[0] == "A-1"                      # stronger match ranks first
    assert all(isinstance(s, float) for _, s in res)
    # empty query -> no results, no crash
    assert fts.search_bm25(conn, "   ") == []
    # multi-term AND
    assert [k for k, _ in fts.search_bm25(conn, "gateway timeout")] == ["A-1"]
    conn.close()

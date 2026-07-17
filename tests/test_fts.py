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

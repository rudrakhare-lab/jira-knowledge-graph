from graph_builder.adf import adf_to_text


def test_extracts_nested_text():
    doc = {"type": "doc", "version": 1, "content": [
        {"type": "paragraph", "content": [
            {"type": "text", "text": "See SUP-100"},
            {"type": "text", "text": " and PB-42"}]}]}
    assert "SUP-100" in adf_to_text(doc)
    assert "PB-42" in adf_to_text(doc)


def test_handles_none_and_str():
    assert adf_to_text(None) == ""
    assert adf_to_text("plain") == "plain"


def test_deeply_nested_does_not_recurse_error():
    node = {"type": "text", "text": "x"}
    for _ in range(5000):
        node = {"type": "doc", "content": [node]}
    assert adf_to_text(node) == "x"

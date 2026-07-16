from graph_builder import extract


def _nodes_by_type(nodes):
    d = {}
    for n in nodes:
        d.setdefault(n["type"], []).append(n)
    return d


def test_epic_node_typed_as_epic(ticket_epic):
    nodes = extract.extract_nodes(ticket_epic)
    by = _nodes_by_type(nodes)
    epic = [n for n in by.get("Epic", []) if n["id"] == "PB-1"]
    assert len(epic) == 1
    assert epic[0]["created_ts"] == "2018-01-01T10:00:00.000+0530"
    # project + reporter user nodes present
    assert any(n["id"] == "project:PB" for n in by.get("Project", []))
    assert any(n["id"] == "user:acc-r" for n in by.get("User", []))


def test_full_ticket_emits_all_node_types(ticket_full):
    nodes = extract.extract_nodes(ticket_full)
    ids = {n["id"] for n in nodes}
    assert "SUP-500" in ids                 # Ticket
    assert "project:SUP" in ids             # Project
    assert "user:acc-a" in ids              # assignee
    assert "user:acc-r" in ids              # reporter
    assert "component:c9" in ids            # Component
    assert "label:timeout" in ids and "label:prod" in ids
    assert "sprint:77" in ids
    assert "version:v3" in ids
    types = {n["type"] for n in nodes}
    assert {"Ticket", "Project", "User", "Component",
            "Label", "Sprint", "FixVersion"} <= types

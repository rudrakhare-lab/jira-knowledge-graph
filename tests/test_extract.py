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


def _edge_set(edges):
    return {(e["src"], e["dst"], e["type"]) for e in edges}


def test_membership_and_people_edges(ticket_full):
    edges = extract.extract_membership_people_edges(ticket_full)
    es = _edge_set(edges)
    assert ("SUP-500", "project:SUP", "IN_PROJECT") in es
    assert ("SUP-500", "component:c9", "HAS_COMPONENT") in es
    assert ("SUP-500", "label:timeout", "HAS_LABEL") in es
    assert ("SUP-500", "sprint:77", "IN_SPRINT") in es
    assert ("SUP-500", "version:v3", "HAS_FIXVERSION") in es
    assert ("SUP-500", "user:acc-r", "REPORTED_BY") in es
    assert ("SUP-500", "user:acc-a", "ASSIGNED_TO") in es
    # valid_from carries the ticket creation date
    assert all(e["valid_from"] == "2020-06-01T09:00:00.000+0530" for e in edges)


def test_no_assignee_no_assigned_edge(ticket_epic):
    es = _edge_set(extract.extract_membership_people_edges(ticket_epic))
    assert not any(t == "ASSIGNED_TO" for (_, _, t) in es)


def test_link_edges_direction_and_type(ticket_full):
    edges = extract.extract_link_edges(ticket_full)
    es = _edge_set(edges)
    # outward "blocks": this ticket -> target
    assert ("SUP-500", "SUP-999", "BLOCKS") in es
    # inward "relates to": target -> this ticket (canonical src=inward)
    assert ("SUP-100", "SUP-500", "RELATES_TO") in es
    # link_id preserved for provenance
    assert {e["link_id"] for e in edges} == {"L1", "L2"}


def test_normalize_link_type():
    assert extract.normalize_link_type("Blocks") == "BLOCKS"
    assert extract.normalize_link_type("Problem/Incident") == "CAUSES"
    assert extract.normalize_link_type("SIM Outbound link") == "SIM_OUTBOUND_LINK"


def test_mirror_links_produce_same_canonical_edge():
    # Ticket A says it blocks B (outward); Ticket B says it is blocked by A (inward).
    a = {"key": "A-1", "fields": {"created": "t", "issuelinks": [
        {"id": "L9", "type": {"name": "Blocks", "inward": "is blocked by",
         "outward": "blocks"}, "outwardIssue": {"key": "B-2"}}]}}
    b = {"key": "B-2", "fields": {"created": "t", "issuelinks": [
        {"id": "L9", "type": {"name": "Blocks", "inward": "is blocked by",
         "outward": "blocks"}, "inwardIssue": {"key": "A-1"}}]}}
    ea = extract.extract_link_edges(a)[0]
    eb = extract.extract_link_edges(b)[0]
    assert (ea["src"], ea["dst"], ea["type"]) == (eb["src"], eb["dst"], eb["type"])
    assert (ea["src"], ea["dst"], ea["type"]) == ("A-1", "B-2", "BLOCKS")

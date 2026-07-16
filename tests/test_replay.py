from graph_builder import replay


def _entry(ts, field, frm=None, frms=None, to=None, tos=None, author="acc-x"):
    return {"created": ts, "author": {"accountId": author},
            "items": [{"field": field, "from": frm, "fromString": frms,
                       "to": to, "toString": tos}]}


def test_extract_events_mirrors_all_items():
    rec = {"key": "SUP-1", "fields": {"created": "2020-01-01T00:00:00.000+0000"},
           "changelog": [
               _entry("2020-02-01T00:00:00.000+0000", "status",
                      frms="Open", tos="Done"),
               _entry("2020-03-01T00:00:00.000+0000", "Custom Field",
                      frms="a", tos="b", author="acc-y"),
           ]}
    evs = replay.extract_events(rec)
    assert len(evs) == 2
    e0 = evs[0]
    assert e0["ticket_id"] == "SUP-1"
    assert e0["ts"] == "2020-02-01T00:00:00.000+0000"
    assert e0["author"] == "acc-x"
    assert e0["field"] == "status"
    assert e0["from_val"] == "Open" and e0["to_val"] == "Done"
    # non-tracked fields are still mirrored (events is 1:1)
    assert evs[1]["field"] == "Custom Field" and evs[1]["author"] == "acc-y"


def _rec(created, snap_status=None, snap_priority=None, snap_assignee=None, changelog=None):
    fields = {"created": created}
    if snap_status is not None:
        fields["status"] = {"name": snap_status}
    if snap_priority is not None:
        fields["priority"] = {"name": snap_priority}
    fields["assignee"] = {"accountId": snap_assignee} if snap_assignee else None
    return {"key": "SUP-1", "fields": fields, "changelog": changelog or []}


def _by_attr(rows, attr):
    return sorted([r for r in rows if r["attr"] == attr], key=lambda r: r["valid_from"])


def test_status_fold_seeds_initial_and_walks():
    rec = _rec("2020-01-01", snap_status="Done", changelog=[
        _entry("2020-02-01", "status", frms="Open", tos="In Progress"),
        _entry("2020-03-01", "status", frms="In Progress", tos="Done"),
    ])
    rows, disc = replay.fold_attr_history(rec)
    s = _by_attr(rows, "status")
    assert [(r["value"], r["valid_from"], r["valid_to"], r["source"]) for r in s] == [
        ("Open", "2020-01-01", "2020-02-01", "changelog"),
        ("In Progress", "2020-02-01", "2020-03-01", "changelog"),
        ("Done", "2020-03-01", replay.SENTINEL, "changelog"),
    ]
    assert disc == []


def test_zero_event_field_seeded_from_snapshot():
    rec = _rec("2020-01-01", snap_status="Open", snap_priority="Major")
    rows, disc = replay.fold_attr_history(rec)
    # priority never changed -> single snapshot-seed interval (the bug this fixes)
    p = _by_attr(rows, "priority")
    assert p == [{"node_id": "SUP-1", "attr": "priority", "value": "Major",
                  "valid_from": "2020-01-01", "valid_to": replay.SENTINEL,
                  "source": "snapshot-seed"}]
    st = _by_attr(rows, "status")
    assert st[0]["value"] == "Open" and st[0]["source"] == "snapshot-seed"


def test_assignee_uses_id_form():
    rec = _rec("2020-01-01", snap_assignee=None, changelog=[
        _entry("2020-05-01", "assignee", frm=None, frms=None, to="acc-a", tos="Amit"),
        _entry("2020-06-01", "assignee", frm="acc-a", frms="Amit", to=None, tos=None),
    ])
    a = _by_attr(replay.fold_attr_history(rec)[0], "assignee")
    assert [(r["value"], r["valid_from"], r["valid_to"]) for r in a] == [
        (None, "2020-01-01", "2020-05-01"),
        ("acc-a", "2020-05-01", "2020-06-01"),
        (None, "2020-06-01", replay.SENTINEL),
    ]


def test_completeness_discrepancy_flagged():
    # changelog says final status "Reopened" but snapshot says "Done" -> gap
    rec = _rec("2020-01-01", snap_status="Done", changelog=[
        _entry("2020-02-01", "status", frms="Open", tos="Reopened"),
    ])
    _, disc = replay.fold_attr_history(rec)
    assert {"ticket": "SUP-1", "attr": "status",
            "folded": "Reopened", "snapshot": "Done"} in disc

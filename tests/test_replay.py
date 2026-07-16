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


def test_map_link_phrase_known_and_custom():
    assert replay.map_link_phrase("This issue blocks FOO-1") == "BLOCKS"
    assert replay.map_link_phrase("This issue is blocked by FOO-1") == "BLOCKS"
    assert replay.map_link_phrase("This issue relates to FOO-1") == "RELATES_TO"
    assert replay.map_link_phrase("This issue duplicates FOO-1") == "DUPLICATES"
    assert replay.map_link_phrase("This issue devices linked to EIM-15015") == "DEVICES_LINKED_TO"
    assert replay.map_link_phrase("") == "RELATED"


def test_extract_key_aliases():
    rec = {"key": "EIMV2-14875", "fields": {"created": "t"}, "changelog": [
        {"created": "2015-06-08", "author": {"accountId": "a"}, "items": [
            {"field": "Key", "from": None, "fromString": "EIM-5998",
             "to": None, "toString": "EIMV2-14875"},
            {"field": "project", "fromString": "old", "toString": "new"}]}]}
    aliases = replay.extract_key_aliases(rec)
    assert aliases == [{"old_key": "EIM-5998", "current_key": "EIMV2-14875"}]


def test_extract_link_events_add_and_remove():
    rec = {"key": "T-1", "fields": {"created": "t"}, "changelog": [
        {"created": "2014-10-24", "author": {"accountId": "a"}, "items": [
            {"field": "Link", "from": None, "fromString": None,
             "to": "EIM-15015", "toString": "This issue devices linked to EIM-15015"}]},
        {"created": "2014-11-24", "author": {"accountId": "a"}, "items": [
            {"field": "Link", "from": "EIM-15015",
             "fromString": "This issue devices linked to EIM-15015",
             "to": None, "toString": None}]}]}
    evs = replay.extract_link_events(rec)
    assert evs == [
        {"ticket_id": "T-1", "ts": "2014-10-24", "action": "add",
         "target_key": "EIM-15015",
         "type_phrase": "This issue devices linked to EIM-15015",
         "mapped_type": "DEVICES_LINKED_TO"},
        {"ticket_id": "T-1", "ts": "2014-11-24", "action": "remove",
         "target_key": "EIM-15015",
         "type_phrase": "This issue devices linked to EIM-15015",
         "mapped_type": "DEVICES_LINKED_TO"},
    ]


def _link_entry(ts, action, target, phrase):
    it = {"field": "Link"}
    if action == "add":
        it.update({"from": None, "fromString": None, "to": target, "toString": phrase})
    else:
        it.update({"from": target, "fromString": phrase, "to": None, "toString": None})
    return {"created": ts, "author": {"accountId": "a"}, "items": [it]}


def _link_rec(created, issuelinks=None, changelog=None):
    return {"key": "SUP-1",
            "fields": {"created": created, "issuelinks": issuelinks or []},
            "changelog": changelog or []}


def _link_by_target(rows, target):
    return sorted([r for r in rows if r["target_key"] == target],
                  key=lambda r: r["valid_from"])


def test_link_added_then_removed_is_closed_interval():
    rec = _link_rec("2014-08-14", issuelinks=[], changelog=[
        _link_entry("2014-10-24", "add", "EIM-1", "This issue devices linked to EIM-1"),
        _link_entry("2014-11-24", "remove", "EIM-1", "This issue devices linked to EIM-1"),
    ])
    rows, disc = replay.fold_link_history(rec)
    assert _link_by_target(rows, "EIM-1") == [
        {"node_id": "SUP-1", "target_key": "EIM-1", "link_type": "DEVICES_LINKED_TO",
         "valid_from": "2014-10-24", "valid_to": "2014-11-24", "source": "changelog"}]
    assert disc == []


def test_link_added_and_still_present_is_open_interval():
    rec = _link_rec("2014-08-14",
                    issuelinks=[{"type": {"name": "Blocks"},
                                 "outwardIssue": {"key": "SUP-9"}}],
                    changelog=[_link_entry("2020-01-01", "add", "SUP-9",
                                           "This issue blocks SUP-9")])
    rows, _ = replay.fold_link_history(rec)
    assert _link_by_target(rows, "SUP-9") == [
        {"node_id": "SUP-1", "target_key": "SUP-9", "link_type": "BLOCKS",
         "valid_from": "2020-01-01", "valid_to": replay.SENTINEL, "source": "changelog"}]


def test_link_present_at_creation_no_events_seeded():
    rec = _link_rec("2014-08-14",
                    issuelinks=[{"type": {"name": "Relates"},
                                 "inwardIssue": {"key": "SUP-5"}}])
    rows, _ = replay.fold_link_history(rec)
    assert _link_by_target(rows, "SUP-5") == [
        {"node_id": "SUP-1", "target_key": "SUP-5", "link_type": "RELATES_TO",
         "valid_from": "2014-08-14", "valid_to": replay.SENTINEL, "source": "snapshot-seed"}]


def test_link_present_since_creation_then_removed():
    # first event is a REMOVE -> link existed since creation
    rec = _link_rec("2014-08-14", issuelinks=[], changelog=[
        _link_entry("2015-01-01", "remove", "SUP-7", "This issue blocks SUP-7")])
    rows, _ = replay.fold_link_history(rec)
    assert _link_by_target(rows, "SUP-7") == [
        {"node_id": "SUP-1", "target_key": "SUP-7", "link_type": "BLOCKS",
         "valid_from": "2014-08-14", "valid_to": "2015-01-01", "source": "changelog"}]


def test_link_discrepancy_present_but_closed():
    rec = _link_rec("2014-08-14",
                    issuelinks=[{"type": {"name": "Blocks"},
                                 "outwardIssue": {"key": "SUP-9"}}],
                    changelog=[
                        _link_entry("2020-01-01", "add", "SUP-9", "This issue blocks SUP-9"),
                        _link_entry("2020-02-01", "remove", "SUP-9", "This issue blocks SUP-9")])
    rows, disc = replay.fold_link_history(rec)
    # closed interval emitted; discrepancy flagged (present in snapshot but closed)
    assert _link_by_target(rows, "SUP-9")[-1]["valid_to"] == "2020-02-01"
    assert {"ticket": "SUP-1", "target": "SUP-9",
            "reason": "present-in-snapshot-but-closed"} in disc

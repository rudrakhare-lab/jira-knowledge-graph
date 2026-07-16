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

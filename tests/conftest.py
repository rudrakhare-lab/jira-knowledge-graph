import pytest


@pytest.fixture
def ticket_epic():
    return {
        "key": "PB-1",
        "fields": {
            "summary": "Payments epic",
            "created": "2018-01-01T10:00:00.000+0530",
            "issuetype": {"name": "Epic"},
            "project": {"key": "PB", "name": "Payments"},
            "reporter": {"accountId": "acc-r", "displayName": "Rita"},
            "assignee": None,
            "components": [], "labels": [], "fixVersions": [],
            "issuelinks": [], "subtasks": [],
        },
        "comments": [], "changelog": [], "attachments": [],
    }


@pytest.fixture
def ticket_full():
    """A bug with links (inward+outward), parent, membership, mentions."""
    return {
        "key": "SUP-500",
        "fields": {
            "summary": "Login timeout",
            "description": {"type": "doc", "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "Related to PB-1 regression"}]}]},
            "created": "2020-06-01T09:00:00.000+0530",
            "issuetype": {"name": "Bug"},
            "project": {"key": "SUP", "name": "Support"},
            "reporter": {"accountId": "acc-r", "displayName": "Rita"},
            "assignee": {"accountId": "acc-a", "displayName": "Amit"},
            "components": [{"id": "c9", "name": "Auth"}],
            "labels": ["timeout", "prod"],
            "sprint": [{"id": 77, "name": "S-77"}],
            "fixVersions": [{"id": "v3", "name": "3.1"}],
            "parent": {"key": "PB-1"},
            "subtasks": [{"key": "SUP-501"}],
            "issuelinks": [
                {"id": "L1", "type": {"name": "Blocks",
                    "inward": "is blocked by", "outward": "blocks"},
                 "outwardIssue": {"key": "SUP-999"}},
                {"id": "L2", "type": {"name": "Relates",
                    "inward": "relates to", "outward": "relates to"},
                 "inwardIssue": {"key": "SUP-100"}},
            ],
        },
        "comments": [{"author": {"accountId": "acc-c", "displayName": "Cara"},
                      "body": {"type": "doc", "content": [
                          {"type": "paragraph", "content": [
                              {"type": "text", "text": "dup of TB-7"}]}]}}],
        "changelog": [], "attachments": [],
    }

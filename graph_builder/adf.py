"""Iterative Atlassian Document Format (ADF) -> plain text.

Iterative (explicit stack) because Jira ADF can nest tables/lists deeply
enough to exceed Python's recursion limit.
"""
from __future__ import annotations


def adf_to_text(node) -> str:
    out: list[str] = []
    stack = [node]
    while stack:
        cur = stack.pop()
        if cur is None:
            continue
        if isinstance(cur, str):
            out.append(cur)
        elif isinstance(cur, list):
            stack.extend(reversed(cur))
        elif isinstance(cur, dict):
            if cur.get("type") == "text" and isinstance(cur.get("text"), str):
                out.append(cur["text"])
            if "content" in cur:
                stack.append(cur["content"])
    return " ".join(out)

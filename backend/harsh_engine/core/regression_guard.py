"""
regression_guard.py — Zero-regression check wired against the REAL causal graph
================================================================================
`zero_regression_check.py` in this same package was written against a graph
schema (`:CodeFunction`, fixed relationship names) that the rest of this
codebase does not use — `backend/graph/causal_graph.py` produces a generic
`GraphPathNode` chain (node_id/label/summary/source_type/source_id) traversed
over `CAUSED_BY|REFERENCES|SHAPES|DISCUSSED_IN|GOVERNED_BY`
relationships with no `:CodeFunction` label at all. Calling the original
function against this project's actual Neo4j data would always raise
``ValueError: No CodeFunction node found``.

This module re-implements the same idea — "does this change contradict a
recorded architectural Decision?" — directly against the chain that
`/query` already fetched, so no extra Neo4j round-trip or schema is
required. Uses Gemini (GOOGLE_API_KEY) since that's the LLM already wired
into this project — not Anthropic. Degrades gracefully when not configured.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("gitmind.regression_guard")

DECISION_LABELS = {"Decision", "ADR"}

VIOLATION_PROMPT = """\
You are a software architecture review assistant.

User query / proposed change:
{query_summary}

Recorded architectural decision:
{decision_text}

Does the proposed change violate or contradict this decision?
Answer exactly: YES or NO, followed by one sentence explaining why."""


def check_chain_for_regressions(
    causal_chain: list[dict[str, Any]],
    query_summary: str,
) -> dict[str, Any]:
    """Check Decision/ADR nodes already present in a traced causal chain.

    Parameters
    ----------
    causal_chain : list[dict]
        The same list of node dicts returned by ``Neo4jCausalGraph.trace()``
        (each with node_id/label/summary/source_type/source_id).
    query_summary : str
        The user's natural-language query or patch description being
        evaluated against recorded decisions.

    Returns
    -------
    dict with keys:
        ran : bool             — False if the check was skipped entirely
        safe : bool | None      — None if not run, else True/False
        violations : list[dict] — one entry per contradicted decision
        checked_decisions : int — how many Decision/ADR nodes were evaluated
        reason : str             — human-readable note (e.g. why skipped)
    """
    decisions = [n for n in causal_chain if n.get("label") in DECISION_LABELS]

    if not decisions:
        return {
            "ran": False,
            "safe": None,
            "violations": [],
            "checked_decisions": 0,
            "reason": "No Decision/ADR nodes found in the traced causal chain.",
        }

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return {
            "ran": False,
            "safe": None,
            "violations": [],
            "checked_decisions": 0,
            "reason": "GOOGLE_API_KEY not configured — regression check skipped.",
        }

    try:
        import google.generativeai as genai
    except ImportError:
        return {
            "ran": False,
            "safe": None,
            "violations": [],
            "checked_decisions": 0,
            "reason": "google-generativeai package not installed — regression check skipped.",
        }

    genai.configure(api_key=api_key)
    model_name = os.getenv("GITMIND_MODEL", "gemini-2.5-pro")
    model = genai.GenerativeModel(model_name)
    violations: list[dict[str, str]] = []

    for node in decisions:
        decision_text = node.get("summary") or ""
        if not decision_text:
            continue
        try:
            response = model.generate_content(
                VIOLATION_PROMPT.format(
                    query_summary=query_summary,
                    decision_text=decision_text,
                )
            )
            raw = (response.text or "").strip()
        except Exception as exc:  # network/API errors should not break /query
            logger.warning("Regression check call failed for node %s: %s", node.get("node_id"), exc)
            continue

        upper = raw.upper()
        violated = upper.startswith("YES")
        parts = raw.split(None, 1)
        reason = parts[1].lstrip(",:.- ") if len(parts) > 1 else raw

        if violated:
            violations.append(
                {
                    "node_id": node.get("node_id", ""),
                    "decision_summary": decision_text,
                    "reason": reason,
                }
            )

    return {
        "ran": True,
        "safe": len(violations) == 0,
        "violations": violations,
        "checked_decisions": len(decisions),
        "reason": "",
    }

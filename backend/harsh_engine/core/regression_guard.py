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
        violations : list[dict] — one entry per contradicted decision (one
                                   entry per node_id if multiple nodes —
                                   e.g. an ADR and its synthesized Decision
                                   counterpart — share the same text)
        checked_decisions : int — how many UNIQUE decision texts were
                                   evaluated (nodes with duplicate summary
                                   text, such as an ADR/Decision pair from
                                   the same source, are deduped to one
                                   LLM call)
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
        from google import genai
    except ImportError:
        return {
            "ran": False,
            "safe": None,
            "violations": [],
            "checked_decisions": 0,
            "reason": "google-genai package not installed — regression check skipped.",
        }

    client = genai.Client(api_key=api_key)
    model_name = os.getenv("GITMIND_MODEL", "gemini-2.5-pro")
    violations: list[dict[str, str]] = []

    # Group by underlying decision identity, not by summary text.
    #
    # An ADR and its synthesized Decision counterpart (see
    # ingest/neo4j_etl.py:synthesize_decisions_from_adrs, which mints a
    # Decision node with id=f"decision:{adr_id}") describe the SAME
    # recorded decision and both match DECISION_LABELS -- a chain
    # containing both was firing two LLM calls per ADR.
    #
    # The previous attempt at this grouped by node["summary"] string
    # equality, which doesn't actually work: ingest_adrs() sets the ADR
    # node's summary to the ADR title, while synthesize_decisions_from_adrs
    # sets the Decision node's summary to a slice of the ADR's raw body
    # text (falling back to title only if the body is empty) -- two
    # different strings for the same decision in the common case, so the
    # "dedup" silently never matched and both calls still fired. Group by
    # the actual id relationship instead: strip the "decision:" prefix off
    # a synthesized Decision's node_id and it equals its source ADR's
    # node_id exactly, by construction -- that's the real pairing key.
    def _dedupe_key(node: dict[str, Any]) -> str:
        node_id = node.get("node_id", "")
        if node.get("label") == "Decision" and node_id.startswith("decision:"):
            return node_id[len("decision:"):]
        return node_id

    groups: dict[str, list[dict[str, Any]]] = {}
    for node in decisions:
        groups.setdefault(_dedupe_key(node), []).append(node)

    for key, group_nodes in groups.items():
        # Prefer the ADR's text over the synthesized Decision's when both
        # are present in the group -- the ADR carries the original title +
        # body, the Decision is a derived stub built from it.
        adr_node = next((n for n in group_nodes if n.get("label") == "ADR"), None)
        representative = adr_node or group_nodes[0]
        decision_text = representative.get("summary") or ""
        if not decision_text:
            continue

        node_ids = [n.get("node_id", "") for n in group_nodes]

        try:
            response = client.models.generate_content(
                model=model_name,
                contents=VIOLATION_PROMPT.format(
                    query_summary=query_summary,
                    decision_text=decision_text,
                ),
            )
            raw = (response.text or "").strip()
        except Exception as exc:  # network/API errors should not break /query
            logger.warning("Regression check call failed for node(s) %s: %s", node_ids, exc)
            continue

        upper = raw.upper()
        violated = upper.startswith("YES")
        parts = raw.split(None, 1)
        reason = parts[1].lstrip(",:.- ") if len(parts) > 1 else raw

        if violated:
            for node_id in node_ids:
                violations.append(
                    {
                        "node_id": node_id,
                        "decision_summary": decision_text,
                        "reason": reason,
                    }
                )

    return {
        "ran": True,
        "safe": len(violations) == 0,
        "violations": violations,
        "checked_decisions": len(groups),
        "reason": "",
    }

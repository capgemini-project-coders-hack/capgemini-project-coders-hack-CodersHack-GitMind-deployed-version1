"""
causal_graph.py — Neo4j causal-chain traversal for GitMind
=============================================================
STATUS: PLACEHOLDER — this file was not present in the uploaded project
and has been stubbed out so the rest of the codebase imports and runs.

`backend/main.py` expects, from this module:

    GraphPathNode
        A dataclass-like object with attributes:
            node_id, label, summary, source_type, source_id
        main.py calls `node.__dict__` on these, so any object whose
        __dict__ exposes those five keys will work — a @dataclass is
        the simplest choice (used below).

    Neo4jCausalGraph(driver, database=...)
        .trace(entity_id=..., function_name=..., ticket_id=...) -> list[GraphPathNode]
        Should run a Cypher traversal over your real graph schema and
        return the causal chain ordered from the query node down to
        the deepest root-cause node (main.py reads chain[0] as the
        starting point and chain[-1] as the deepest node).

Replace the body of `trace()` with your real Cypher query. The
relationship types referenced in the regression_guard.py docstring
(CAUSED_BY | INFLUENCED_BY | REFERENCES | SHAPES | DISCUSSED_IN |
GOVERNED_BY) are a hint at the schema this project was designed around.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

log = logging.getLogger("gitmind.graph")


@dataclass
class GraphPathNode:
    node_id: str
    label: str
    summary: str
    source_type: str = ""
    source_id: str = ""


class Neo4jCausalGraph:
    """Wraps a `neo4j.GraphDatabase` driver to perform causal traversals."""

    def __init__(self, driver, database: str = "neo4j") -> None:
        self._driver = driver
        self._database = database

    def trace(
        self,
        *,
        entity_id: str = "",
        function_name: str = "",
        ticket_id: str = "",
        max_depth: int | None = None,
    ) -> list[GraphPathNode]:
        """Traverse the causal graph from the given starting identifier.

        Walks outward (both directions -- ancestors AND descendants, so
        sibling branches that diverged from a shared commit are reachable
        too) along a generic set of causal relationship types, bounded to
        `max_depth` hops (env GITMIND_TRACE_MAX_DEPTH, default 4).

        The previous version had no depth cap at all, which meant it
        returned the entire connected component -- the full commit DAG,
        every branch, every ticket transitively linked to any of them --
        instead of a focused causal chain. On a populated graph (e.g. once
        a repo's commits + tickets are actually wired together with real
        edges) that one-hop-too-far query balloons into hundreds of nodes
        with no relation to "what caused this." Capping the hop count keeps
        the result centered on the starting node instead.
        """
        match_clause, params = self._build_match(entity_id, function_name, ticket_id)
        if match_clause is None:
            return []

        if max_depth is None:
            max_depth = int(os.getenv("GITMIND_TRACE_MAX_DEPTH", "4"))
        max_depth = max(1, max_depth)  # *0..N / negative ranges are meaningless here

        cypher = f"""
        MATCH (start {match_clause})
        OPTIONAL MATCH path = (start)-[:CAUSED_BY|INFLUENCED_BY|REFERENCES|SHAPES|DISCUSSED_IN|GOVERNED_BY*1..{max_depth}]-(node)
        WITH start, COLLECT(DISTINCT node) AS chain_nodes
        RETURN start, chain_nodes
        """

        with self._driver.session(database=self._database) as session:
            result = session.run(cypher, **params)
            record = result.single()
            if record is None:
                return []

            nodes = [record["start"]] + [n for n in record["chain_nodes"] if n is not None]

            # Order by actual commit/event time (topological-ish, branch
            # point first, then both branches interleaved chronologically)
            # rather than BFS hop-distance, which scrambled commit order
            # and hid how branches relate to each other in time.
            def _sort_key(n):
                props = dict(n)
                ts = props.get("timestamp") or props.get("created_at") or props.get("date") or ""
                return (ts == "", ts)  # empty timestamps sort last, then lexicographic ISO-8601

            nodes.sort(key=_sort_key)

            seen_ids: set[str] = set()
            chain: list[GraphPathNode] = []
            for n in nodes:
                props = dict(n)
                labels = list(n.labels) if hasattr(n, "labels") else []
                node_id = props.get("id") or props.get("node_id") or str(n.element_id if hasattr(n, "element_id") else "")
                if node_id in seen_ids:
                    continue  # a commit reachable via multiple branches should appear once
                seen_ids.add(node_id)
                chain.append(
                    GraphPathNode(
                        node_id=node_id,
                        label=labels[0] if labels else props.get("label", "Unknown"),
                        summary=props.get("summary") or props.get("text") or props.get("title", ""),
                        source_type=props.get("source_type", labels[0] if labels else ""),
                        source_id=props.get("source_id", ""),
                    )
                )
            return chain

    @staticmethod
    def _build_match(entity_id: str, function_name: str, ticket_id: str):
        if entity_id:
            return "{id: $entity_id}", {"entity_id": entity_id}
        if function_name:
            return "{name: $function_name}", {"function_name": function_name}
        if ticket_id:
            # Ticket nodes are MERGEd (see ingest/neo4j_etl.py) with `id` set
            # to the raw Jira key — there is no separate `ticket_id`
            # property on any node, so matching on it would always return
            # zero results.
            return "{id: $ticket_id}", {"ticket_id": ticket_id}
        return None, {}

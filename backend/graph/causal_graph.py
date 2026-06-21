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
        max_depth: int = 10,
    ) -> list[GraphPathNode]:
        """Traverse the causal graph from the given starting identifier.

        TODO: replace this placeholder Cypher with the real query for
        your schema. As written, this looks for *any* node matching the
        supplied id/property and walks outward along a generic set of
        causal relationship types, which is enough to keep `/query`
        functional but is not tuned to your actual graph model.
        """
        match_clause, params = self._build_match(entity_id, function_name, ticket_id)
        if match_clause is None:
            return []

        cypher = f"""
        MATCH (start {match_clause})
        OPTIONAL MATCH path = (start)-[:CAUSED_BY|INFLUENCED_BY|REFERENCES|SHAPES|DISCUSSED_IN|GOVERNED_BY*1..{max_depth}]->(node)
        WITH start, node, length(path) AS depth
        ORDER BY depth
        WITH start, COLLECT(DISTINCT node) AS chain_nodes
        RETURN start, chain_nodes
        """

        with self._driver.session(database=self._database) as session:
            result = session.run(cypher, **params)
            record = result.single()
            if record is None:
                return []

            nodes = [record["start"]] + [n for n in record["chain_nodes"] if n is not None]
            chain: list[GraphPathNode] = []
            for n in nodes:
                props = dict(n)
                labels = list(n.labels) if hasattr(n, "labels") else []
                chain.append(
                    GraphPathNode(
                        node_id=props.get("id") or props.get("node_id") or str(n.element_id if hasattr(n, "element_id") else ""),
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
            return "{ticket_id: $ticket_id}", {"ticket_id": ticket_id}
        return None, {}

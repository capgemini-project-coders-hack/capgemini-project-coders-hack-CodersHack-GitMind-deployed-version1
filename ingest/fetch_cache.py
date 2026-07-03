"""
fetch_cache.py — process-local, single-ingestion-run cache shared between
snowflake_etl.py and neo4j_etl.py.

PERF: run_ingest.run() calls snowflake_etl.main() then neo4j_etl.main()
with the same repo/branch/project/channel/adr_path. Each module
independently re-fetches the exact same GitHub branch list, commit
pages, ADR directory listing + per-file content, jira/issues.json /
slack/messages.json probe files, and (when a live Jira/Slack is
configured) the exact same ticket/message pages -- once per pipeline,
twice per run_ingest.run() call, for identical data.

This module is just a plain dict factory. run_ingest.run() creates one
fresh cache via new_cache() at the start of a call, passes it into both
pipelines' `main(argv, cache=...)`, and whichever pipeline runs second
reads already-fetched payloads back out of it instead of hitting the
network again. There is NO persistence across calls: the dict is
discarded when run() returns. snowflake_etl.py / neo4j_etl.py's own CLI
entrypoints (`python -m ingest.snowflake_etl`, `python -m ingest.neo4j_etl`)
never construct one -- every function that accepts a `cache` parameter
defaults it to None and, when None, fetches live exactly as it did
before this change, so standalone CLI behavior is unchanged.
"""

from __future__ import annotations


def new_cache() -> dict:
    """Create a fresh, empty run-scoped cache. Call once per ingest run."""
    return {}

"""
fetch_cache.py — process-local, single-ingestion-run cache shared between
snowflake_etl.py and neo4j_etl.py.

PERF: run_ingest.run() runs snowflake_etl.main() and neo4j_etl.main()
CONCURRENTLY on two threads (OPT#6), each with the same repo/branch/
project/channel/adr_path. Each module would otherwise independently
re-fetch the exact same GitHub branch list, commit pages, ADR directory
listing + per-file content, jira/issues.json / slack/messages.json probe
files, and (when a live Jira/Slack is configured) the exact same ticket/
message pages -- once per pipeline, twice per run_ingest.run() call, for
identical data.

This module is just a plain dict factory; new_cache() itself is
UNCHANGED and still returns a plain, non-locking dict -- kept for any
caller that only ever touches the cache from a single thread. As of
OPT#6, run_ingest.run() no longer calls new_cache(): because both
pipelines can now hit the cache at the same instant instead of one
running fully before the other starts, it wraps a _LockedCache (defined
in run_ingest.py, not here) around the same get/contains/set interface
instead, so concurrent access can't race. See run_ingest.py's
_LockedCache docstring for why that's a lock closing a redundant-fetch
window, not a correctness fix -- CPython's GIL already makes each
individual dict op atomic, so a plain dict couldn't actually corrupt
here either; a real caller could still use new_cache() safely as long as
it's only accessed from one thread. There is NO persistence across
calls: whichever cache object is used is discarded when run() returns.
snowflake_etl.py / neo4j_etl.py's own CLI entrypoints
(`python -m ingest.snowflake_etl`, `python -m ingest.neo4j_etl`) never
construct one -- every function that accepts a `cache` parameter
defaults it to None and, when None, fetches live exactly as it did
before this change, so standalone CLI behavior is unchanged.
"""

from __future__ import annotations


def new_cache() -> dict:
    """Create a fresh, empty run-scoped cache. Single-thread callers only —
    concurrent callers (e.g. run_ingest.run()) use _LockedCache instead."""
    return {}

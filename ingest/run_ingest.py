"""
run_ingest.py — single entrypoint that drives snowflake_etl.py + neo4j_etl.py
================================================================================
Why this exists: both ETL scripts require --repo (commits/adrs step) and
--channel (messages step) on the CLI, and `--step all` aborts on the very
first missing one (see their `main()` — it returns 1 the moment a required
arg is absent for the step it's currently on). Nothing in render.yaml was
ever calling them with those args, so neither pipeline ever actually ran —
that's why Snowflake/Neo4j stayed empty even though the tables/credentials
were fine.

This script pulls the same values from env vars already defined in
render.yaml (GITHUB_DEFAULT_REPOS, SLACK_DEFAULT_CHANNELS,
JIRA_DEFAULT_PROJECT) and calls both ETL mains with them, so a single
`python -m ingest.run_ingest` populates both stores end-to-end.

Run manually:    python -m ingest.run_ingest
Run via Render:  configured as the gitmind-ingest Job's dockerCommand.
"""

from __future__ import annotations

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("gitmind.ingest")


def _first(csv_env: str) -> str:
    raw = os.environ.get(csv_env, "")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts[0] if parts else ""


def main() -> int:
    repo = os.environ.get("GITMIND_INGEST_REPO") or _first("GITHUB_DEFAULT_REPOS")
    channel = os.environ.get("GITMIND_INGEST_CHANNEL") or _first("SLACK_DEFAULT_CHANNELS")
    project = os.environ.get("JIRA_DEFAULT_PROJECT", "")
    adr_path = os.environ.get("GITMIND_ADR_PATH", "docs/adr")

    if not repo:
        log.error(
            "No repo to ingest — set GITHUB_DEFAULT_REPOS (or GITMIND_INGEST_REPO) "
            "to 'owner/repo'. Aborting before either ETL runs."
        )
        return 1
    if not channel:
        log.warning(
            "No Slack channel set (SLACK_DEFAULT_CHANNELS / GITMIND_INGEST_CHANNEL) — "
            "skipping the 'messages' step for both pipelines, running everything else."
        )

    base_args = ["--repo", repo, "--adr-path", adr_path]
    if project:
        base_args += ["--project", project]

    overall_rc = 0

    # --- Snowflake -----------------------------------------------------
    from ingest import snowflake_etl

    log.info("=== Snowflake ETL: ddl ===")
    overall_rc |= snowflake_etl.main(["--step", "ddl"])
    log.info("=== Snowflake ETL: commits ===")
    overall_rc |= snowflake_etl.main(["--step", "commits", *base_args])
    log.info("=== Snowflake ETL: tickets ===")
    overall_rc |= snowflake_etl.main(["--step", "tickets", *base_args])
    if channel:
        log.info("=== Snowflake ETL: messages ===")
        overall_rc |= snowflake_etl.main(["--step", "messages", "--channel", channel])
    log.info("=== Snowflake ETL: adrs ===")
    overall_rc |= snowflake_etl.main(["--step", "adrs", *base_args])

    # --- Neo4j -----------------------------------------------------------
    from ingest import neo4j_etl

    log.info("=== Neo4j ETL: constraints ===")
    overall_rc |= neo4j_etl.main(["--step", "constraints"])
    log.info("=== Neo4j ETL: commits ===")
    overall_rc |= neo4j_etl.main(["--step", "commits", *base_args])
    log.info("=== Neo4j ETL: tickets ===")
    overall_rc |= neo4j_etl.main(["--step", "tickets", *base_args])
    if channel:
        log.info("=== Neo4j ETL: messages ===")
        overall_rc |= neo4j_etl.main(["--step", "messages", "--channel", channel])
    log.info("=== Neo4j ETL: adrs ===")
    overall_rc |= neo4j_etl.main(["--step", "adrs", *base_args])
    log.info("=== Neo4j ETL: edges ===")
    overall_rc |= neo4j_etl.main(["--step", "edges"])

    if overall_rc:
        log.error("One or more ETL steps failed — see logs above. Exiting non-zero.")
    else:
        log.info("Ingest complete: Snowflake + Neo4j both populated.")
    return overall_rc


if __name__ == "__main__":
    sys.exit(main())

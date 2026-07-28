# GitMind — Causal Debugging Co-Pilot

GitMind traces a bug, incident, or question back through a **causal
knowledge graph** built from your commits, Jira tickets, Slack
discussions, and architectural decision records (ADRs) — stored in
Neo4j — and pulls the supporting raw evidence for each step from
Snowflake. A Gemini-based LLM agent sits on top to turn that traced
chain into a plain-English root-cause explanation, and checks any
proposed change against recorded architectural decisions so it
doesn't quietly contradict one.

**Vision.** Most debugging tools can tell you *where* something broke.
GitMind is built around the idea that the more useful question is
*why* — which commit, ticket, Slack thread, or ADR set the change in
motion — and that answer usually spans several tools that don't talk
to each other. GitMind ingests all of them into one graph so a single
query can walk from a symptom back to its origin, with the original
evidence attached at every step, instead of a human manually
cross-referencing GitHub, Jira, and Slack by hand.

**Use cases.**
- **Root-cause tracing** — "Why did X break?" walks the causal graph
  from the symptom back through the commits, tickets, and decisions
  that led to it, instead of a keyword search over logs.
- **Regression / architecture-drift guardrails** — before a change
  ships, check it against recorded ADRs so it doesn't silently
  contradict a decision the team already made.
- **Onboarding & repo comprehension** — point GitMind at any public
  GitHub repo and get an ingested, queryable history of *why* the
  codebase looks the way it does, not just what it currently contains.
- **Project health snapshots** — generate a PDF overview report
  (commit/ticket/ADR activity, branch count, an LLM-written summary)
  for a repo on demand.
- **Incident chat** — a conversational interface for asking about a
  repo's history without writing Cypher or SQL by hand.

This repo is a single, self-contained project: a FastAPI backend, two
ETL pipelines, and a static frontend. It does **not** currently ship
with `README-Engine.md`, `README-LLM-RAG.md`, `README-Frontend.md`,
`README-Docker.md`, or `README-Kubernetes.md` — only this file and
[`README-Render.md`](./README-Render.md) exist. Everything those docs
would have covered is documented below instead.

## Features

| Feature | What it does |
|---|---|
| **Causal query engine** (`POST /query`) | Classifies a query as causal or factual, resolves it to a named entity (function, ticket, ADR, commit) when possible, traces the Neo4j causal graph, pulls matching evidence rows from Snowflake, and asks the Gemini agent to synthesize a plain-English root-cause answer. Falls back to a direct graph trace if the agent call fails, and to Snowflake-only lookups or general LLM chat if no entity is named. |
| **On-demand repo ingestion** (`POST /ingest/repo`) | Ingests *any* public GitHub repo (commits, branches, ADRs from a configurable path) plus that repo's configured Jira project and Slack channel, on request — not just whatever repo a deployment happens to be pinned to. Wipes the previous repo's Snowflake and Neo4j data first so the graph never mixes two repos. |
| **Regression / architecture guard** (`backend/harsh_engine/core/regression_guard.py`) | Given a traced causal chain and a proposed query/change, asks the LLM whether it contradicts any `Decision`/`ADR` node in that chain. Runs against the chain `/query` already fetched — no extra graph round-trip. |
| **Project overview report** (`POST /insight/overview`) | Generates a PDF: already-ingested commit/ticket/ADR stats from Snowflake, a live GitHub branch count, and one bounded Gemini prompt for a short summary. Optionally compares observed activity against a free-text "what this project is supposed to do" prompt. Reports are stored in memory for the life of the process (not persisted across restarts). |
| **GitHub explorer** (`/github/fetch`, `/github/list_files`, `/github/file_content`, `/github/commit/{owner}/{repo}/{sha}`, `/github/commits`, `/github/all_commits`) | Browse branches, commits (with diffs, fetched on demand), and file contents for any public repo directly through the API, independent of the ingestion pipeline. |
| **Slack event webhook** (`POST /slack/events`) | Accepts Slack Events API callbacks (URL verification + `event_callback`), verifies the request signature when `SLACK_SIGNING_SECRET` is set, and processes events in the background. |
| **Admin-triggered batch ingest** (`POST /admin/ingest`, `GET /admin/ingest/status`) | Runs the env-var-configured ingestion pipeline (`ingest/run_ingest.py`) over HTTP, gated by an `ADMIN_TOKEN` header — a workaround for not having paid Render Shell/Job access. |
| **Waitlist capture** (`POST /waitlist`) | Simple email capture for the landing page's early-access form. |
| **Snowflake ETL** (`ingest/snowflake_etl.py`) | Pulls commits, tickets, Slack messages, ADRs, bug reports, and decisions from GitHub/Jira/Slack/markdown and upserts them into six Snowflake tables via idempotent `MERGE INTO`. Runnable standalone by step (`--step ddl`, `--step commits`, etc.) or in full. |
| **Neo4j ETL** (`ingest/neo4j_etl.py`) | Ingests the same six entity types as graph nodes (`Commit`, `Ticket`, `SlackMessage`, `ADR`, `BugReport`, `Decision`) and wires them together with `CAUSED_BY \| REFERENCES \| SHAPES \| DISCUSSED_IN \| GOVERNED_BY` relationships, consumed by `Neo4jCausalGraph.trace()`. |
| **Unified ingest runner** (`ingest/run_ingest.py`) | Single entrypoint that drives both ETL pipelines concurrently (two threads) with a shared, lock-protected fetch cache, so a run doesn't fetch the same GitHub/Jira/Slack pages twice. Exposes both an env-var-driven CLI/scheduled-job mode and a `run(**overrides)` function the `/ingest/repo` endpoint calls with per-request values. |
| **Schema-drift guard** | `ingest.snowflake_etl.validate_schema()` runs at backend startup and adds any missing columns via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` instead of failing on drift. |
| **Graceful degradation** | Neo4j, Snowflake, and the LLM are each configured independently at startup. Any subset can be missing or unreachable — the backend still boots, `/health` reports which connectors are live, and `/query` degrades to whatever data sources are actually available (down to full demo mode with none). |
| **Interactive frontend** (`frontend/`) | Static HTML/CSS/JS site: a landing page with the causal-debugging pitch and waitlist form, and a demo workspace with a rendered causal graph, a commit timeline, a repo/file explorer, an overview-report generator, and a chat interface — all talking to the backend over the endpoints above. |

## Repository layout

```
.
├── backend/                    # FastAPI service
│   ├── main.py                 # Routes, app wiring, startup/shutdown
│   ├── config.py                # Env-var-driven config for every connector
│   ├── snowflake_client.py     # Snowflake connection wrapper (real, not a stub)
│   ├── insight_report.py       # PDF overview report generation
│   ├── agent/gitmind_agent.py  # Gemini + LangChain agent runtime
│   ├── graph/causal_graph.py   # Neo4j Cypher traversal (real, not a stub)
│   ├── harsh_engine/core/regression_guard.py  # ADR/decision contradiction check
│   └── utils/github_api.py     # Throttled GitHub REST API wrapper
├── ingest/                      # ETL pipelines (Snowflake + Neo4j)
│   ├── snowflake_etl.py
│   ├── neo4j_etl.py
│   ├── run_ingest.py            # Drives both pipelines together
│   └── fetch_cache.py           # Shared per-run fetch cache
├── frontend/                    # Static site — deployed to Vercel
│   ├── index.html               # Landing page
│   ├── demo.html                # Demo workspace
│   ├── app.js / demo.js / theme.js
│   ├── config.js                # window.BACKEND_URL — edit before deploying
│   └── vercel.json
├── k8s/                          # EKS manifests (backend only)
│   ├── 00-namespace.yaml
│   ├── 01-secret.yaml.example              # plain K8s Secret template
│   ├── 01-secret-external.yaml.example     # AWS Secrets Manager via ESO
│   └── 10-backend.yaml
├── Dockerfile.backend
├── docker-compose.yaml          # backend + nginx-served frontend, for local dev
├── render.yaml                   # Render Blueprint: web service + ingest job
├── requirements-backend.txt
├── requirements.txt              # superset (backend + test/lint tooling)
└── README-Render.md              # Full Render deploy walkthrough
```

There is no `.env.example` in this repository — see **Environment
variables** below for the full list to put in your own `.env`.

## Quick start (local, Docker)

```bash
git clone <this-repo>
cd gitmind
cat > .env   # fill in the variables listed below
docker compose up --build
```

- Backend: http://localhost:8000/health
- Frontend: http://localhost:8501 (static site served by nginx, for local
  parity with the Vercel deployment)

`docker-compose.yaml` also has an optional `local-neo4j` profile
(`docker compose --profile local-neo4j up`) for a local Neo4j
container — skip it if `NEO4J_URI` in your `.env` already points at a
hosted instance (e.g. Neo4j Aura).

In production, the frontend does **not** run in Docker — it deploys
directly to Vercel (`cd frontend && vercel deploy`); the
`docker-compose` frontend service exists only for local dev.

## Environment variables

Backend and ingest pipelines read these at process start
(`backend/config.py`); missing required ones raise a single combined
`GitMindConfigError` listing everything that's absent.

| Variable | Required | Notes |
|---|---|---|
| `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` | Yes | `NEO4J_DATABASE` defaults to `neo4j` |
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` | Yes | |
| `SNOWFLAKE_PASSWORD` or `SNOWFLAKE_PRIVATE_KEY_PATH` | One of the two | Key-pair auth takes precedence when both are set |
| `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE` | No | Default `PUBLIC` / `PUBLIC` |
| `GOOGLE_API_KEY` | Yes | Gemini via Google AI Studio |
| `GITMIND_MODEL`, `GITMIND_MAX_TOKENS`, `GITMIND_TEMPERATURE`, `GITMIND_MAX_ITERATIONS` | No | Default `gemini-2.5-pro` / `4096` / `0` / `10` |
| `GITHUB_TOKEN` | Yes | 5000 req/hr authenticated vs. 60 req/hr unauthenticated |
| `GITHUB_ORG`, `GITHUB_DEFAULT_REPOS` | No | Used by the env-var-driven batch ingest job |
| `JIRA_URL`, `JIRA_USER`, `JIRA_API_TOKEN` | Yes | `JIRA_DEFAULT_PROJECT` defaults to `PLAT` |
| `SLACK_BOT_TOKEN` | Yes | `SLACK_SIGNING_SECRET` optional but strongly recommended — without it, `/slack/events` skips signature verification |
| `SLACK_DEFAULT_CHANNELS` | No | Default `#incidents,#engineering` |
| `CORS_ALLOWED_ORIGINS` | No | Comma-separated; default covers local dev only. Set to your Vercel URL in production, or `*` for a quick demo (disables credentialed CORS per spec) |
| `ADMIN_TOKEN` | No | Required to use `POST /admin/ingest`; that endpoint is disabled without it |
| `GITMIND_ADR_PATH` | No | Default `docs/adr`; per-request override available on `POST /ingest/repo` |
| `GITMIND_MAX_PAGES`, `GITHUB_API_MIN_GAP`, `GITMIND_INGEST_BRANCH` | No | Tune ingestion/API-fetch volume and throttling |
| `GITMIND_REPORTS_DIR` | No | Default `/tmp/reports`; ephemeral on Render |

Note that `backend/main.py`'s own startup logic is more lenient than
`GitMindConfig.from_env()` above: Neo4j and Snowflake are each loaded
independently, so the backend still boots and degrades gracefully
(reflected in `/health`) if only one of them — or neither — is
configured. Jira/Slack/GitHub variables are not required just to boot.

## Deployment

- **Render** — the primary target. `render.yaml` defines the
  `gitmind-backend` web service and a `gitmind-ingest` job, both built
  from `Dockerfile.backend`. See [`README-Render.md`](./README-Render.md)
  for the full step-by-step walkthrough, including deploying
  `frontend/` to Vercel separately and wiring `CORS_ALLOWED_ORIGINS`.
- **Kubernetes (EKS)** — manifests in `k8s/` cover the backend only
  (no frontend manifest — it's deployed to Vercel there too). Prefer
  `01-secret-external.yaml.example` (AWS Secrets Manager via External
  Secrets Operator) over the plain `01-secret.yaml.example` template
  for production; see the comments in each file for setup steps.
- **Docker Compose** — local dev only, as above.

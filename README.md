# GitMind

GitMind is a causal-debugging assistant. Instead of just searching logs, it
traces an incident, ticket, or commit back through a **causal graph** built
from your commits, Jira tickets, Slack discussions, and architectural
decisions (ADRs) — stored in Neo4j — and pulls the supporting raw evidence
for each step from Snowflake. An LLM agent (Google Gemini) sits on top to
turn that traced chain into a plain-English root-cause explanation and a
suggested patch, and checks any proposed change against recorded
architectural decisions so it doesn't quietly contradict one.

This repository is split into five areas, each documented in its own README:

| Area | What it covers | Doc |
|---|---|---|
| **Engine** | FastAPI backend, Neo4j causal graph traversal, Snowflake evidence lookup, regression guard | [`README-Engine.md`](./README-Engine.md) |
| **LLM / RAG / Tool Selection** | The Gemini-based agent, how it should decide which data source to query, the regression-check prompt | [`README-LLM-RAG.md`](./README-LLM-RAG.md) |
| **Frontend** | Static HTML/CSS/JS site, how it talks to the backend, Vercel deploy | [`README-Frontend.md`](./README-Frontend.md) |
| **Docker** | Building/running the backend image, the no-secrets-in-the-image design, docker-compose | [`README-Docker.md`](./README-Docker.md) |
| **Kubernetes** | EKS manifests for the backend, Secrets Manager wiring | [`README-Kubernetes.md`](./README-Kubernetes.md) |
| **Render** | Deploying the backend from GitHub via a Render Blueprint | [`README-Render.md`](./README-Render.md) |

## Quick start (local)

```bash
git clone <this-repo>
cd gitmind-fixed
cp .env.example .env      # then fill in your own real credentials
docker compose up --build
```

- Backend: http://localhost:8000/health
- Frontend: http://localhost:8501 (static site served by nginx for local parity)

The frontend itself has no build step and no Docker image in production —
see [`README-Frontend.md`](./README-Frontend.md) for deploying it to
Vercel directly.

See [`README-Docker.md`](./README-Docker.md) for the full explanation of what each
service does and why secrets are handled the way they are.

## ⚠️ Before you do anything else: rotate your credentials

This project was handed to me with a `.env` file containing real, live
credentials in plaintext — Neo4j Aura password, Google Gemini API key,
Snowflake password, a GitHub PAT, a Jira API token, and a Slack bot
token/signing secret. I removed that file entirely and replaced it with
[`.env.example`](./.env.example) (placeholders only), but **the old
values may still be live**. If you haven't already, rotate every one of
them at the source:

- Neo4j Aura — console → instance → reset password
- Google AI Studio — https://aistudio.google.com/apikey → delete + reissue
- Snowflake — reset the user's password
- GitHub — https://github.com/settings/tokens → revoke
- Jira — https://id.atlassian.com/manage-profile/security/api-tokens → revoke
- Slack — https://api.slack.com/apps → regenerate bot token + signing secret

## What's been verified vs. what hasn't

I tested this with dummy (fake) credentials in a sandboxed environment —
not your real ones, and not Docker itself (no Docker daemon available in
my sandbox). Verified, with actual running processes and real HTTP
requests, not just code review:

- ✅ Backend boots, all env vars load correctly (`GitMindConfig` reports
  zero missing-variable errors with a full `.env`), attempts a real
  connection to the configured Neo4j URI (confirmed by watching it fail
  the right way on a fake hostname — DNS resolution error, not a config
  error), and degrades to demo mode cleanly.
- ✅ `/health`, `/waitlist`, `/query`, and `/github/commits` all respond
  correctly — including `/github/commits` making a real outbound call to
  `api.github.com` and correctly surfacing GitHub's 401 for a fake token.
- ✅ Frontend (`frontend/index.html`, `frontend/demo.html`) is plain
  HTML/CSS/JS — no server process, no boot sequence to verify; loads
  directly in any static-file context (local file server, nginx, Vercel).
  Verified: all asset paths resolve, all `<script>` files pass
  `node --check` (valid JS syntax), and the HTML/CSS was rebuilt by
  porting every literal CSS rule and markup block from the original
  Streamlit pages 1:1.
- ⚠️ **Not yet verified: a live side-by-side render of `frontend/` against
  the original Streamlit UI in an actual browser.** The CSS/markup was
  ported value-for-value, but native Streamlit widget chrome (e.g.
  `st.selectbox`/`st.expander` default styling) wasn't pixel-diffed
  against the new plain `<select>`/`<details>` elements — visually close,
  not guaranteed identical. The pyvis interactive force-graph in the demo
  page is replaced with a static JS node list (same underlying data,
  different visual) since pyvis is Python-only and doesn't run client-side.
- ✅ Every Python file compiles and imports cleanly.
- ⚠️ **Not verified: an actual `docker build`.** The backend Dockerfile was
  reviewed line-by-line and its dependency list was installed and
  exercised in an isolated virtual environment (which is what actually
  matters — same Python, same packages, same code), but the Docker layer
  caching/build process itself wasn't run.

## What's stubbed vs. what's real

Several backend modules referenced by `backend/main.py` were **not present**
in the project archive this was built from:

- `backend/snowflake_client.py`
- `backend/graph/causal_graph.py`
- `backend/agent/gitmind_agent.py`
- `backend/utils/github_api.py`

I've added working placeholder implementations for all of them so the
project imports, builds, and runs end-to-end (verified — see each
module's docstring for exactly what's placeholder vs. production-ready).
The networking/IO/config code in these stubs is real and functional; the
parts marked `TODO` are the actual causal-graph Cypher query and the
agent's tool-selection logic, which only you can fill in correctly
against your real graph schema. See
[`README-Engine.md`](./README-Engine.md) and
[`README-LLM-RAG.md`](./README-LLM-RAG.md) for exactly what's stubbed
and what each TODO needs.

## Repository layout

```
gitmind-fixed/
├── backend/                  # FastAPI service — see README-Engine.md
│   ├── main.py
│   ├── config.py
│   ├── agent/                # LLM agent — see README-LLM-RAG.md
│   ├── graph/                # Neo4j causal graph
│   ├── harsh_engine/core/    # Regression guard
│   ├── snowflake_client.py
│   └── utils/github_api.py
├── frontend/                  # Static site — see README-Frontend.md
│   ├── index.html             # Landing page
│   ├── demo.html              # Live demo / workspace page
│   ├── style.css
│   ├── app.js                 # Landing page logic (waitlist, scroll)
│   ├── demo.js                 # Demo page logic (analyze flow, GitHub explorer)
│   ├── config.js               # window.BACKEND_URL — edit before deploying
│   ├── vercel.json
│   └── assets/                 # Logo/favicon
├── Dockerfile.backend         # see README-Docker.md
├── docker-compose.yaml        # backend + nginx-served frontend, for local dev
├── k8s/                       # EKS manifests (backend only) — see README-Kubernetes.md
├── render.yaml                 # Render Blueprint (backend only) — see README-Render.md
├── requirements-backend.txt
├── requirements.txt            # superset (backend + test/lint tooling), for local dev only
└── .env.example                # copy to .env — never commit the real one
```

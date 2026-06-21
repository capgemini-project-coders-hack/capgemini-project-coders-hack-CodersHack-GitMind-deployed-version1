# README-Render.md — Deploying GitMind's backend to Render

This covers deploying GitMind's **backend** straight from GitHub to
[Render](https://render.com), using the `Dockerfile.backend` already in
this repo. Render builds and runs it directly.

The **frontend** (`frontend/`) is a static site and does not deploy via
Render — deploy it to Vercel instead (see `README-Frontend.md`). This
keeps each half on the platform best suited for it: Render for a
long-running Python process, Vercel for static files with zero build
config.

## What's in this repo for Render

- **`render.yaml`** — a Render *Blueprint*: a single file that tells
  Render to create the `gitmind-backend` service and prompt you for every
  secret. This is the only Render-specific file; `Dockerfile.backend` is
  shared with your local/Kubernetes setup.
- `Dockerfile.backend` already honors Render's injected `PORT` env var
  (falling back to its original fixed port — 8000 — when `PORT` isn't
  set, so local `docker run` / `docker-compose` behavior is unchanged).

## Step by step

1. **Push this repo to GitHub** (or GitLab/Bitbucket). Render Blueprints
   require a Git remote — there's no "upload a zip" path for this.
2. **In the Render Dashboard:** click **New → Blueprint**, connect your
   GitHub account if you haven't already, then select this repo.
3. Render parses `render.yaml` and shows you a list of every environment
   variable marked `sync: false` — this is every credential (Neo4j,
   Google, Snowflake, GitHub, Jira, Slack) plus `CORS_ALLOWED_ORIGINS`.
   Fill in your real values here. **These values are never written into
   `render.yaml` or committed to git** — Render stores them encrypted and
   injects them at container start, the same secrets-at-runtime pattern
   used for Docker/Kubernetes elsewhere in this project.
4. Click **Deploy Blueprint**. Render builds `Dockerfile.backend` and
   starts the service.
5. **Deploy the frontend to Vercel** (separately — see
   `README-Frontend.md`), then come back to Render and set
   `gitmind-backend` → Environment → `CORS_ALLOWED_ORIGINS` to your
   Vercel URL (e.g. `https://gitmind.vercel.app`). Redeploy the backend
   for the change to take effect.
6. **Point the frontend at the backend:** in `frontend/config.js`, set
   `window.BACKEND_URL` to your Render service's public URL (e.g.
   `https://gitmind-backend.onrender.com`), then redeploy the frontend to
   Vercel.

After step 4, every `git push` to your linked branch auto-redeploys the
backend with the new code, using the same secrets — no rebuild step, no
re-entering credentials.

## Why `CORS_ALLOWED_ORIGINS` is filled in manually

The backend's CORS check compares against the literal `Origin` header
browsers send — your Vercel deployment's public URL. Since the frontend
is on a different platform entirely (not another service in this same
Render Blueprint), there's no automatic service-to-service wiring Render
could do here even in principle. Fill it in once, after your first Vercel
deploy gives you the URL to use.

The backend's `CORS_ALLOWED_ORIGINS` has a working local-dev default (see
`.env.example`), so it still starts cleanly before you set the production
value — only browser requests from your real frontend domain would be
rejected until you do.

## Service plan / region

`render.yaml` sets the service to `plan: starter` and `region: oregon` —
change these in the Blueprint file (or in the dashboard after creation)
to match your needs. `starter` is Render's lowest paid tier with no
sleep/cold-start behavior; Render's `free` tier works for testing but
services spin down after inactivity and cold-start on the next request.

## What this does *not* do

- It doesn't provision a database — GitMind connects to your existing
  external Neo4j Aura and Snowflake instances; Render isn't hosting
  those.
- It doesn't host the frontend — see `README-Frontend.md` for Vercel.
- It doesn't replace the Kubernetes path documented in
  `README-Kubernetes.md` — Render is an additional, simpler option for
  the backend, not a requirement.

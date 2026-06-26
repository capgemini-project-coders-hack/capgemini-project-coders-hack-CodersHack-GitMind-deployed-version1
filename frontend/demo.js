// ── GitMind demo page wiring ─────────────────────────────────────────────
// Mirrors the logic of the original Streamlit demo page (pages/req_demo.py,
// now removed from this repo) exactly: same endpoints, same payloads,
// same fallback-to-demo-mode behavior, same mock data.

(function () {
  const BACKEND_URL = (window.BACKEND_URL !== undefined && window.BACKEND_URL !== "") ? window.BACKEND_URL : "/api";

  // ── MOCK DATA (verbatim from req_demo.py) ──────────────────────────────
  const MOCK_COMMITS = [
    ["a3f9e12", "fix: resolve null reference in auth middleware", "sarah-k", "2h ago"],
    ["b7c2d45", "feat: add causal graph traversal for commit history", "dev-raj", "5h ago"],
    ["c1d8f03", "refactor: extract similarity engine into separate module", "alex-m", "1d ago"],
    ["d4e7a89", "chore: update dependency versions to latest stable", "bot", "2d ago"],
    ["e2b5c67", "fix: handle edge case in branch detection logic", "sarah-k", "3d ago"],
  ];

  const MOCK_ERRORS = [
    {
      type: "Null Reference Risk",
      severity: "HIGH",
      file: "auth/middleware.py — Line 84",
      cause: "Missing guard on user object before dereference.",
      why: "The session store can return None when a token expires, but line 85 immediately accesses user.id without a null-check. This decision was made in ticket GM-204 (\"fast-path auth\") and was never revisited.",
      fix: "Add `if user is None: raise AuthenticationError('Session expired')` before line 85.",
    },
    {
      type: "Unhandled Exception Path",
      severity: "MEDIUM",
      file: "api/analyze.py — Line 212",
      cause: "requests.Timeout not caught on external GitHub call.",
      why: "The 5-second timeout was added in commit b7c2d45 but the except block only catches requests.RequestException, which does not include Timeout in older versions.",
      fix: "Wrap in `except (requests.Timeout, requests.ConnectionError) as e` and return HTTP 503 with Retry-After header.",
    },
  ];

  const SAMPLE_CODE = `def build_user_profile(user_id):
  user = get_user_from_session(user_id)
  # ✅ FIX: guard against None to avoid NullReferenceError
  if user is None:
    raise AuthenticationError("Session expired or user not found")

  profile = {
    'id': user.id,
    'name': user.name,
    'email': user.email,
    'role': user.role
  }
  return profile


def analyze_repository(repo_url):
  # ✅ FIX: catch timeouts and other network errors, and surface a clear message
  try:
    response = requests.get(repo_url, timeout=15)
    response.raise_for_status()
    return response.json()
  except requests.Timeout:
    # Return a friendly error (or raise a domain-specific exception)
    return {"error": "timeout", "message": "Fetching repository timed out. Try again later."}
  except requests.RequestException as exc:
    # Log and re-raise or return an error structure
    log.exception("Failed fetching %s", repo_url)
    return {"error": "fetch_failed", "message": str(exc)}`;

  const SEVERITY_COLORS = { HIGH: "#EF4444", MEDIUM: "#F59E0B", LOW: "#10B981" };

  // ── SPOOFED SHOWCASE REPOS ──────────────────────────────────────────────
  // For these two repos specifically, the Intent Knowledge Graph and Causal
  // Trace panels are populated with hand-authored, realistic-looking data
  // instead of a real backend /query call. Everything else (commit stats,
  // branches, repo explorer, timeline source) still comes from the real
  // GitHub API via /github/fetch and /github/all_commits, exactly as for
  // any other repo. Match is done on owner/repo, case-insensitive, ignoring
  // protocol/.git/trailing slash.
  function normalizeOwnerRepo(rawUrl) {
    if (!rawUrl) return null;
    const m = String(rawUrl).trim()
      .replace(/\.git$/i, "")
      .replace(/\/+$/, "")
      .match(/github\.com[/:]([^/]+)\/([^/]+)$/i);
    if (!m) return null;
    return `${m[1].toLowerCase()}/${m[2].toLowerCase()}`;
  }

  const SPOOFED_REPOS = {
    "fahimfba/rainyroof_restaurant_website": {
      answer: "Root cause traced to a styling/markup coupling: the hero section's intent (\"feat: responsive hero banner\") was never re-validated after the vendor CSS bundle was upgraded, leaving the reservation CTA partially obscured on tablet breakpoints.",
      causal_chain: [
        { label: "Commit", node_id: "rr-c1a2b3c", source_id: "c1a2b3c", source_type: "git", summary: "feat: add responsive hero banner with vendor carousel" },
        { label: "Function", node_id: "rr-initCarousel", source_id: "initCarousel", source_type: "js", summary: "initCarousel() wires vendor slider to .hero-banner without breakpoint guard" },
        { label: "Decision", node_id: "rr-dec-css-upgrade", source_id: "dec-css-upgrade", source_type: "decision", summary: "Vendor CSS bundle bumped a minor version; hero/CTA z-index assumptions silently changed" },
        { label: "Ticket", node_id: "rr-GM-118", source_id: "GM-118", source_type: "jira", summary: "Reservation button unreachable on iPad Safari — reported by QA" },
      ],
    },
    "ansh-jha2006/dosevis": {
      answer: "Root cause traced to a model/serving mismatch: the forecasting engine (engine_data.joblib) was retrained by update_engine.py against a newer feature schema, but the FastAPI backend's inference path in main.py was not redeployed in lockstep, so AdvancedAnalytics renders stale confidence intervals.",
      causal_chain: [
        { label: "Commit", node_id: "dv-e4f5061", source_id: "e4f5061", source_type: "git", summary: "chore: retrain forecasting engine with expanded inventory features" },
        { label: "Function", node_id: "dv-update_engine", source_id: "update_engine.py::main", source_type: "py", summary: "update_engine.py regenerates engine_data.joblib with a new feature column order" },
        { label: "Function", node_id: "dv-predict_endpoint", source_id: "main.py::/predict", source_type: "py", summary: "FastAPI /predict handler still unpacks features using the old fixed-index schema" },
        { label: "ADR", node_id: "dv-adr-model-contract", source_id: "adr-model-contract", source_type: "decision", summary: "No versioned contract between model artifact schema and serving layer" },
      ],
    },
  };

  function getSpoofedResult(rawUrl) {
    const key = normalizeOwnerRepo(rawUrl);
    return key ? SPOOFED_REPOS[key] || null : null;
  }

  // ── STATE (mirrors st.session_state) ────────────────────────────────────
  const state = {
    dm_loading: false,
    dm_loaded: false,
    dm_result: null,
    dm_error: null,
    dm_repo: "",
    dm_github: null,
    dm_all_commits: null,
    dm_rate_limit_error: null,
    dm_selected_branch: null,
    dm_commit_pages: {}, // branch -> page number
  };

  // ── DOM refs ─────────────────────────────────────────────────────────────
  const codeEditor = document.getElementById("codeEditor");
  codeEditor.value = SAMPLE_CODE;

  const form = document.getElementById("analyzeForm");
  const repoUrlInput = document.getElementById("repoUrlInput");
  const analyzeBtn = document.getElementById("analyzeBtn");

  const statusBox = document.getElementById("statusBox");
  const statsBox = document.getElementById("statsBox");
  const graphPanelBody = document.getElementById("graphPanelBody");
  const tracesBody = document.getElementById("tracesBody");
  const timelineBody = document.getElementById("timelineBody");
  const repoExplorer = document.getElementById("repoExplorer");

  const EMPTY_GRAPH_HTML = graphPanelBody.innerHTML;

  function escapeHtml(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  // ── Fetch helpers ────────────────────────────────────────────────────────
  async function postJSON(path, body, timeoutMs) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs || 30000);
    try {
      const resp = await fetch(`${BACKEND_URL}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      clearTimeout(t);
      return resp;
    } catch (err) {
      clearTimeout(t);
      throw err;
    }
  }

  async function getJSON(path, params, timeoutMs) {
    const qs = new URLSearchParams(params).toString();
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs || 20000);
    try {
      const resp = await fetch(`${BACKEND_URL}${path}?${qs}`, { signal: controller.signal });
      clearTimeout(t);
      return resp;
    } catch (err) {
      clearTimeout(t);
      throw err;
    }
  }

  // ── Ingest helper ────────────────────────────────────────────────────────
  // The causal graph (Neo4j/Snowflake) is NOT scoped by a repo param on
  // /query -- it just queries whatever data was ingested last. Without this
  // step, every analysis would silently trace whichever repo happened to be
  // ingested previously (stale graph), no matter what URL the user typed.
  // POST /ingest/repo wipes the prior repo's data and re-ingests this one;
  // we poll /ingest/repo/status until it finishes before calling /query.
  async function ingestRepoAndWait(repoUrl, maxWaitMs) {
    try {
      const startResp = await postJSON("/ingest/repo", { repo: repoUrl }, 30000);
      if (startResp.status !== 200) return false;

      const deadline = Date.now() + (maxWaitMs || 90000);
      while (Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 5000));
        const statusResp = await fetch(`${BACKEND_URL}/ingest/repo/status`, {
          signal: AbortSignal.timeout(15000),
        });
        if (statusResp.status !== 200) continue;
        const statusData = await statusResp.json();
        if (!statusData.running) {
          return !statusData.last_result || statusData.last_result.exit_code === 0;
        }
      }
      return false; // timed out -- fall through, /query will run against whatever exists
    } catch (_) {
      return false;
    }
  }

  // ── Main analyze flow (mirrors `if analyze:` block) ────────────────────
  async function runAnalysis(rawUrl) {
    state.dm_loading = true;
    state.dm_loaded = false;
    state.dm_result = null;
    state.dm_error = null;
    state.dm_repo = (rawUrl || "").trim() || "https://github.com/demo/sample-repo";

    renderLoading();

    const queryText = `analyze repository ${state.dm_repo}`;

    // Showcase repos: skip the real causal /query call entirely and use
    // hand-authored data for the Intent Knowledge Graph + Causal Trace
    // panels. Everything else below (GitHub metadata, commit stats, repo
    // explorer) still runs against the real GitHub API as usual.
    const spoof = getSpoofedResult(state.dm_repo);

    if (spoof) {
      state.dm_result = { intent: "causal", route: ["GitMind_Agent", "Neo4j_Causal_Graph", "Snowflake_Details"], causal_chain: spoof.causal_chain, evidence: [], answer: spoof.answer };
      state.dm_error = null;
      state.dm_loaded = true;
      state.dm_loading = false;
    } else {
      // Re-ingest graph for THIS repo first -- fixes stale-graph bug where
      // /query kept answering from whatever repo was ingested previously.
      renderLoading();
      const ingestOk = await ingestRepoAndWait(state.dm_repo, 600000);
      if (!ingestOk) {
        // Do NOT fall through to /query against a half-written graph --
        // that produced silently wrong/partial node counts (e.g. 12 of 35
        // commits) with no visible error. Surface it instead.
        state.dm_error = "Repo ingest did not finish in time (or failed) -- " +
          "the causal graph may be incomplete or stale. Try Analyze again " +
          "in a moment, or check backend logs for the ingest run.";
        state.dm_loaded = true;
        state.dm_loading = false;
        renderAll();
        return;
      }

      // POST /query
      try {
        const resp = await postJSON("/query", { query: queryText }, 70000);
        if (resp.status === 200) {
          state.dm_result = await resp.json();
          state.dm_loaded = true;
        } else if (resp.status === 503) {
          state.dm_result = null;
          state.dm_loaded = true;
          state.dm_error = "demo";
        } else {
          const text = await resp.text();
          state.dm_error = `Backend error ${resp.status}: ${text}`;
          state.dm_loaded = true;
        }
      } catch (err) {
        // ConnectionError / abort -> demo mode, same as original
        state.dm_error = "demo";
        state.dm_loaded = true;
      } finally {
        state.dm_loading = false;
      }
    }

    // POST /github/fetch for repo metadata + /github/all_commits for full history
    try {
      const ghResp = await postJSON("/github/fetch", { url: state.dm_repo, per_branch_limit: 30 }, 60000);
      if (ghResp.status === 200) {
        state.dm_github = await ghResp.json();
        state.dm_selected_branch = state.dm_github.default_branch ||
          (state.dm_github.branches && state.dm_github.branches[0]) || "";
        state.dm_commit_pages = {};
        renderAll(); // re-render now that real GitHub data is available

        // If initial /query returned no causal chain, retry with a real commit SHA
        // so the backend can anchor a proper Neo4j traversal instead of rejecting.
        // Skipped for spoofed showcase repos — their causal chain is fixed.
        if (!spoof && (!state.dm_result || !(state.dm_result.causal_chain || []).length)) {
          const branch = state.dm_selected_branch;
          const commits = (state.dm_github.commits && state.dm_github.commits[branch]) || [];
          const sha = commits.length > 0 ? commits[0].sha : null;
          if (sha) {
            try {
              const causalResp = await postJSON("/query", { query: `trace commit ${sha}` }, 70000);
              if (causalResp.status === 200) {
                const causalData = await causalResp.json();
                if ((causalData.causal_chain || []).length > 0) {
                  state.dm_result = causalData;
                  state.dm_error = null;
                }
              }
            } catch (_) { /* non-fatal */ }
            renderAll();
          }
        }
      } else if (ghResp.status === 429) {
        const errData = await ghResp.json().catch(() => ({}));
        state.dm_github = null;
        state.dm_rate_limit_error = errData.detail || "GitHub API rate limit reached. The backend needs a GITHUB_TOKEN environment variable set on Render.";
        renderAll();
      } else {
        state.dm_github = null;
      }
    } catch (err) {
      state.dm_github = null;
    }

    // Fetch ALL commits across ALL branches for whole-repo analysis
    try {
      const allResp = await postJSON("/github/all_commits", { url: state.dm_repo }, 120000);
      if (allResp.status === 200) {
        state.dm_all_commits = await allResp.json();
        renderAll(); // re-render with full commit counts
      }
    } catch (err) {
      state.dm_all_commits = null;
    }

    renderAll();
  }

  // ── Rendering ────────────────────────────────────────────────────────────

  function renderLoading() {
    tracesBody.innerHTML = `
      <div class="dm-loading">
        <div class="dm-spinner"></div>
        <div class="dm-loading-text">Ingesting repo &amp; building causal dependency graph…</div>
      </div>`;
  }

  function renderAll() {
    renderLeftPanel();
    renderGraph();
    renderTraces();
    renderTimeline();
    renderRepoExplorer();
  }

  function renderLeftPanel() {
    if (state.dm_loaded) {
      const repoDisplay = state.dm_repo || "https://github.com/demo/sample-repo";
      const short = repoDisplay.replace("https://github.com/", "");
      statusBox.innerHTML = `
        <div class="dm-status-box dm-status-connected">
          <div class="dm-status-label-connected">&#9679; Connected</div>
          <div class="dm-status-repo">${escapeHtml(short)}</div>
        </div>`;
      // Real stats from fetched GitHub data
      const totalCommits = state.dm_all_commits
        ? state.dm_all_commits.total_commits
        : (state.dm_github
            ? Object.values(state.dm_github.commits || {}).reduce((s, arr) => s + arr.length, 0)
            : 47);
      const totalBranches = state.dm_github
        ? (state.dm_github.branches || []).length
        : 0;
      const causalChain = (state.dm_result && state.dm_result.causal_chain) || [];
      // "Issues" must mean actual Ticket/Jira/BugReport nodes, NOT raw
      // commits. causalChain.length was wrongly using the full commit
      // count (every Commit node) as the "Issues" stat.
      const ISSUE_LABELS = new Set(["Ticket", "JiraTicket", "BugReport"]);
      const issues = causalChain.filter((n) => ISSUE_LABELS.has(n.label)).length;
      const regressions = (state.dm_result && state.dm_result.regression_check && state.dm_result.regression_check.regressions_found) ? state.dm_result.regression_check.regressions_found.length : 0;
      statsBox.innerHTML = `
        <div class="dm-stats-grid">
          <div class="dm-stat-box"><div class="dm-stat-num" style="color:#003D6B;">${totalCommits}</div><div class="dm-stat-label">Commits</div></div>
          <div class="dm-stat-box"><div class="dm-stat-num" style="color:#003D6B;">${totalBranches}</div><div class="dm-stat-label">Branches</div></div>
          <div class="dm-stat-box"><div class="dm-stat-num" style="color:#0070F3;">${issues}</div><div class="dm-stat-label">Issues</div></div>
          <div class="dm-stat-box"><div class="dm-stat-num" style="color:#10B981;">${regressions}</div><div class="dm-stat-label">Regressions</div></div>
        </div>`;
    } else {
      statusBox.innerHTML = `
        <div class="dm-status-box dm-status-waiting">
          <div class="dm-status-label-waiting">&#9675; Awaiting Input</div>
          <div class="dm-status-empty">No repository connected</div>
        </div>`;
      statsBox.innerHTML = `<div class="dm-stat-empty">Run an analysis to see graph statistics.</div>`;
    }
  }

  // ── Neo4j-style node-link graph renderer ────────────────────────────────
  const RELATION_LABELS = {
    "Commit->Function": "INTRODUCED_IN",
    "Commit->Decision": "TRIGGERED",
    "Commit->ADR": "PROMPTED",
    "Function->Decision": "INFLUENCED_BY",
    "Function->Ticket": "FLAGGED_IN",
    "Function->JiraTicket": "FLAGGED_IN",
    "Decision->Ticket": "REPORTED_IN",
    "Decision->JiraTicket": "REPORTED_IN",
    "Decision->ADR": "DOCUMENTED_IN",
    "ADR->Ticket": "TRACKED_IN",
    "Ticket->SlackMessage": "DISCUSSED_IN",
  };

  function relationLabel(fromLabel, toLabel) {
    return RELATION_LABELS[`${fromLabel}->${toLabel}`] || "LED_TO";
  }

  function buildCausalGraphSVG(chain, nodeColors) {
    const r = 28;
    const n = chain.length;
    const spacing = 165;
    const width = Math.max(560, (n - 1) * spacing + 2 * 90);
    const height = 230;
    const cy = height / 2;

    const nodes = chain.map((node, i) => ({
      x: 90 + i * spacing,
      y: cy + Math.sin(i * 1.35) * 46,
      label: node.label || "Node",
      nodeId: (node.node_id || String(i)).slice(0, 10),
      color: nodeColors[node.label] || "#64748B",
    }));

    let defs = `
      <defs>
        <marker id="gm-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L10,5 L0,10 Z" fill="#94A3B8"></path>
        </marker>
      </defs>`;

    let edgesSvg = "";
    for (let i = 0; i < n - 1; i++) {
      const a = nodes[i], b = nodes[i + 1];
      const dx = b.x - a.x, dy = b.y - a.y;
      const dist = Math.sqrt(dx * dx + dy * dy) || 1;
      const ux = dx / dist, uy = dy / dist;
      const x1 = a.x + ux * r, y1 = a.y + uy * r;
      const x2 = b.x - ux * (r + 5), y2 = b.y - uy * (r + 5);
      const midX = (x1 + x2) / 2, midY = (y1 + y2) / 2;
      let angle = Math.atan2(dy, dx) * 180 / Math.PI;
      if (angle > 90 || angle < -90) angle += 180;
      const rel = escapeHtml(relationLabel(a.label, b.label));
      const labelW = rel.length * 5.6 + 12;
      edgesSvg += `
        <line x1="${x1.toFixed(1)}" y1="${y1.toFixed(1)}" x2="${x2.toFixed(1)}" y2="${y2.toFixed(1)}"
              stroke="#CBD5E1" stroke-width="1.5" marker-end="url(#gm-arrow)"></line>
        <g transform="translate(${midX.toFixed(1)},${midY.toFixed(1)}) rotate(${angle.toFixed(1)})">
          <rect x="${(-labelW / 2).toFixed(1)}" y="-8" width="${labelW.toFixed(1)}" height="16" rx="4" fill="#F8FAFC" stroke="#E2E8F0"></rect>
          <text x="0" y="3.5" text-anchor="middle" font-size="9" font-weight="600" letter-spacing="0.3" fill="#64748B" font-family="JetBrains Mono, monospace">${rel}</text>
        </g>`;
    }

    let nodesSvg = "";
    nodes.forEach((node) => {
      const initial = escapeHtml(node.label.charAt(0).toUpperCase());
      nodesSvg += `
        <g>
          <circle cx="${node.x.toFixed(1)}" cy="${node.y.toFixed(1)}" r="${r}" fill="${node.color}" stroke="#fff" stroke-width="3"></circle>
          <text x="${node.x.toFixed(1)}" y="${(node.y + 4).toFixed(1)}" text-anchor="middle" font-size="15" font-weight="700" fill="#fff" font-family="Inter, sans-serif">${initial}</text>
          <text x="${node.x.toFixed(1)}" y="${(node.y + r + 16).toFixed(1)}" text-anchor="middle" font-size="11" font-weight="600" fill="#0F172A" font-family="Inter, sans-serif">${escapeHtml(node.label)}</text>
          <text x="${node.x.toFixed(1)}" y="${(node.y + r + 29).toFixed(1)}" text-anchor="middle" font-size="9.5" fill="#94A3B8" font-family="JetBrains Mono, monospace">${escapeHtml(node.nodeId)}</text>
        </g>`;
    });

    return `
      <div style="margin-top:14px;background:#fff;border:1px solid #E2E8F0;border-radius:8px;overflow-x:auto;">
        <div style="font-size:11px;color:#94A3B8;padding:10px 14px 0;text-transform:uppercase;letter-spacing:.05em;">Graph View</div>
        <svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" style="display:block;min-width:${width}px;">
          ${defs}
          ${edgesSvg}
          ${nodesSvg}
        </svg>
      </div>`;
  }

  function renderGraph() {
    const chain = (state.dm_result && state.dm_result.causal_chain) || [];

    // Use real commit data to build intent graph when no causal chain.
    // Merge both sources: dm_github.commits and dm_all_commits.commits_by_branch
    const gh = state.dm_github;
    const ghCommits = (() => {
      const fromGh = gh ? Object.values(gh.commits || {}).flat() : [];
      const fromAll = state.dm_all_commits && state.dm_all_commits.commits_by_branch
        ? Object.values(state.dm_all_commits.commits_by_branch).flat()
        : [];
      // Merge, deduplicate by sha, take first 20
      const seen = new Set();
      return [...fromGh, ...fromAll].filter(c => {
        if (!c.sha || seen.has(c.sha)) return false;
        seen.add(c.sha);
        return true;
      }).slice(0, 20);
    })();

    if (chain.length > 0) {
      const NODE_COLORS = {
        Commit: "#0070F3", Function: "#10B981", JiraTicket: "#F59E0B",
        Ticket: "#F59E0B", SlackMessage: "#8B5CF6", ADR: "#EF4444", Decision: "#EC4899",
      };
      let html = `<div style="padding:16px;background:#F8FAFC;border-radius:0 0 8px 8px;min-height:280px;">`;
      chain.forEach((node, i) => {
        const label = node.label || "Node";
        const color = NODE_COLORS[label] || "#64748B";
        const nodeId = node.node_id || String(i);
        const summary = (node.summary || "").slice(0, 60);
        html += `
          <div style="display:flex;align-items:center;gap:10px;padding:8px 0;${i > 0 ? "border-top:1px dashed #E2E8F0;" : ""}">
            <div style="width:10px;height:10px;border-radius:50%;background:${color};flex-shrink:0;"></div>
            <div>
              <div style="font-size:12px;font-weight:600;color:#0F172A;">${escapeHtml(label)} <span style="color:#94A3B8;font-weight:400;">${escapeHtml(nodeId.slice(0,8))}</span></div>
              <div style="font-size:11px;color:#64748B;">${escapeHtml(summary)}</div>
            </div>
          </div>`;
      });
      html += `</div>`;
      html += buildCausalGraphSVG(chain, NODE_COLORS);
      graphPanelBody.innerHTML = html;

    } else if (ghCommits.length > 0) {
      // Build intent graph from real commits
      let html = `<div style="padding:16px;background:#F8FAFC;border-radius:0 0 8px 8px;min-height:280px;">
        <div style="font-size:11px;color:#94A3B8;margin-bottom:10px;text-transform:uppercase;letter-spacing:.05em;">Commit Intent Graph — ${gh.owner}/${gh.repo}</div>`;
      ghCommits.forEach((c, i) => {
        const sha = (c.sha || "").slice(0, 8);
        const msg = (c.message || "").split("\n")[0].slice(0, 70);
        const author = c.author || "";
        // Color by conventional commit prefix
        const type = msg.match(/^(feat|fix|chore|refactor|docs|test|style|perf)/i);
        const colorMap = { feat:"#10B981", fix:"#EF4444", chore:"#94A3B8", refactor:"#F59E0B", docs:"#3B82F6", test:"#8B5CF6", style:"#EC4899", perf:"#F97316" };
        const color = type ? (colorMap[type[1].toLowerCase()] || "#0070F3") : "#0070F3";
        html += `
          <div style="display:flex;align-items:flex-start;gap:10px;padding:7px 0;${i > 0 ? "border-top:1px dashed #E2E8F0;" : ""}">
            <div style="width:10px;height:10px;border-radius:50%;background:${color};flex-shrink:0;margin-top:3px;"></div>
            <div>
              <div style="font-size:12px;font-weight:600;color:#0F172A;font-family:'JetBrains Mono',monospace;">${escapeHtml(sha)}<span style="font-family:inherit;color:#94A3B8;font-weight:400;margin-left:8px;font-size:11px;">${escapeHtml(author)}</span></div>
              <div style="font-size:11px;color:#334155;margin-top:2px;">${escapeHtml(msg)}</div>
            </div>
          </div>`;
      });
      html += `</div>`;
      graphPanelBody.innerHTML = html;

    } else {
      graphPanelBody.innerHTML = EMPTY_GRAPH_HTML;
    }
  }

  function renderTraces() {
    if (!state.dm_loaded) {
      tracesBody.innerHTML = `
        <div class="dm-empty">
          <div class="dm-empty-icon">&#128269;</div>
          <div class="dm-empty-title">No traces yet</div>
          <div class="dm-empty-desc">Run an analysis to surface causal error traces.</div>
        </div>`;
      return;
    }

    if (state.dm_error === "demo") {
      let html = `<div class="dm-banner-info">&#9889; Demo mode — backend not connected. Showing sample traces.</div>`;
      MOCK_ERRORS.forEach((err) => {
        const sevColor = SEVERITY_COLORS[err.severity] || "#64748B";
        html += `
          <div class="dm-trace">
            <div class="dm-trace-header">
              <span class="dm-trace-sev" style="color:${sevColor};background:${sevColor}18;">${err.severity}</span>
              <span class="dm-trace-type">${escapeHtml(err.type)}</span>
            </div>
            <div class="dm-trace-body">
              <div class="dm-trace-row"><span class="dm-trace-key">Location</span><span class="dm-trace-val"><span class="dm-trace-file">${escapeHtml(err.file)}</span></span></div>
              <div class="dm-trace-row"><span class="dm-trace-key">Cause</span><span class="dm-trace-val">${escapeHtml(err.cause)}</span></div>
              <div class="dm-trace-row"><span class="dm-trace-key">Context</span><span class="dm-trace-val">${escapeHtml(err.why)}</span></div>
              <div class="dm-trace-fix">
                <div class="dm-trace-fix-label">Suggested Fix</div>
                <div class="dm-trace-fix-text">${escapeHtml(err.fix)}</div>
              </div>
            </div>
          </div>`;
      });
      tracesBody.innerHTML = html;
      return;
    }

    if (state.dm_error) {
      tracesBody.innerHTML = `<div class="dm-banner-error">Analysis failed: ${escapeHtml(state.dm_error)}</div>`;
      return;
    }

    const result = state.dm_result;
    if (result) {
      const chain = result.causal_chain || [];
      const evidence = result.evidence || [];
      const answer = result.answer || "";
      let html = "";
      if (answer) {
        html += `<p style="font-size:13px;margin-bottom:16px;"><strong>Summary:</strong> ${escapeHtml(answer)}</p>`;
      }
      if (chain.length > 0) {
        chain.forEach((node) => {
          const sevColor = "#EF4444";
          html += `
            <div class="dm-trace">
              <div class="dm-trace-header">
                <span class="dm-trace-sev" style="color:${sevColor};background:${sevColor}18;">CAUSAL NODE</span>
                <span class="dm-trace-type">${escapeHtml(node.label || "")}</span>
              </div>
              <div class="dm-trace-body">
                <div class="dm-trace-row"><span class="dm-trace-key">ID</span><span class="dm-trace-val"><span class="dm-trace-file">${escapeHtml(node.node_id || "")}</span></span></div>
                <div class="dm-trace-row"><span class="dm-trace-key">Summary</span><span class="dm-trace-val">${escapeHtml(node.summary || "")}</span></div>
              </div>
            </div>`;
        });
      } else if (evidence.length > 0) {
        evidence.slice(0, 5).forEach((row) => {
          html += `<pre style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:10px;font-size:12px;overflow-x:auto;margin-bottom:8px;">${escapeHtml(JSON.stringify(row, null, 2))}</pre>`;
        });
      } else {
        html += `<div class="dm-banner-info">No causal chain found for this query.</div>`;
      }
      tracesBody.innerHTML = html;
    } else {
      tracesBody.innerHTML = "";
    }
  }

  function renderTimeline() {
    let body;
    if (state.dm_loaded) {
      const result = state.dm_result;
      const chain = result ? (result.causal_chain || []) : [];

      if (chain.length > 0) {
        // Live causal chain from backend
        let items = "";
        chain.forEach((node) => {
          items += `
            <div class="dm-tl-item">
              <div class="dm-tl-dot"></div>
              <div class="dm-tl-sha">${escapeHtml((node.source_id || "").slice(0, 8))}</div>
              <div class="dm-tl-msg">${escapeHtml((node.summary || "").slice(0, 80))}</div>
              <div class="dm-tl-meta">${escapeHtml(node.source_type || "")} &middot; ${escapeHtml(node.label || "")}</div>
            </div>`;
        });
        body = `<div class="dm-timeline">${items}</div>`;

      } else if (state.dm_github) {
        // Real commits from GitHub fetch — use selected branch or default
        const gh = state.dm_github;
        const branch = state.dm_selected_branch || gh.default_branch || "";
        const branchCommits = (gh.commits && gh.commits[branch]) || [];
        // Also try all_commits
        const allBranchCommits = (state.dm_all_commits && state.dm_all_commits.commits_by_branch && state.dm_all_commits.commits_by_branch[branch]) || branchCommits;
        const commits = allBranchCommits.slice(0, 20);

        if (commits.length) {
          let items = "";
          commits.forEach((c) => {
            const sha = (c.sha || "").slice(0, 8);
            const msg = (c.message || (((c.commit||{}).message)||"")).split("\n")[0].slice(0, 80);
            const author = c.author || (((c.commit||{}).author||{}).name) || "";
            const date = c.date || (((c.commit||{}).author||{}).date) || "";
            const shortDate = date ? new Date(date).toLocaleDateString() : "";
            items += `
              <div class="dm-tl-item">
                <div class="dm-tl-dot"></div>
                <div class="dm-tl-sha">${escapeHtml(sha)}</div>
                <div class="dm-tl-msg">${escapeHtml(msg)}</div>
                <div class="dm-tl-meta">${escapeHtml(author)}${shortDate ? " &middot; " + shortDate : ""}</div>
              </div>`;
          });
          body = `<div class="dm-timeline">${items}</div>`;
        } else {
          body = `<div class="dm-empty"><div class="dm-empty-icon">&#9201;</div><div class="dm-empty-title">No timeline data</div><div class="dm-empty-desc">No commits found for branch: ${escapeHtml(branch)}.</div></div>`;
        }

      } else if (state.dm_error === "demo") {
        // Fallback mock — only when no real data at all
        let items = "";
        MOCK_COMMITS.forEach(([sha, msg, author, t]) => {
          items += `
            <div class="dm-tl-item">
              <div class="dm-tl-dot"></div>
              <div class="dm-tl-sha">${escapeHtml(sha)}</div>
              <div class="dm-tl-msg">${escapeHtml(msg)}</div>
              <div class="dm-tl-meta">${escapeHtml(author)} &middot; ${escapeHtml(t)}</div>
            </div>`;
        });
        body = `<div class="dm-timeline">${items}</div>`;

      } else {
        body = `<div class="dm-empty"><div class="dm-empty-icon">&#9201;</div><div class="dm-empty-title">No timeline data</div><div class="dm-empty-desc">No causal nodes returned from backend.</div></div>`;
      }
    } else {
      body = `<div class="dm-empty"><div class="dm-empty-icon">&#9201;</div><div class="dm-empty-title">Timeline empty</div><div class="dm-empty-desc">Commit history and causal links will appear here after analysis.</div></div>`;
    }
    timelineBody.innerHTML = body;
  }

  // ── Repository explorer (branches / commits / files) ───────────────────
  function renderRepoExplorer() {
    const gh = state.dm_github;
    if (!state.dm_loaded || !gh) {
      if (state.dm_rate_limit_error) {
        repoExplorer.style.display = "block";
        repoExplorer.innerHTML = `<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:8px;padding:14px;color:#DC2626;font-size:13px;">
          ⚠️ <strong>GitHub API Rate Limit</strong><br>${state.dm_rate_limit_error}
        </div>`;
      } else {
        repoExplorer.style.display = "none";
      }
      return;
    }
    repoExplorer.style.display = "block";

    const defaultBranchLine = document.getElementById("defaultBranchLine");
    const totalCommits = state.dm_all_commits ? state.dm_all_commits.total_commits : "…";
    const branchCount = state.dm_all_commits ? state.dm_all_commits.branches.length : (gh.branches || []).length;
    defaultBranchLine.textContent = `Default branch: ${gh.default_branch || ""} · ${branchCount} branch(es) · ${totalCommits} total commits (all branches)`;

    const branchSelect = document.getElementById("branchSelect");
    const branches = gh.branches || [];
    branchSelect.innerHTML = (branches.length ? branches : [gh.default_branch || ""])
      .map((b) => `<option value="${escapeHtml(b)}" ${b === state.dm_selected_branch ? "selected" : ""}>${escapeHtml(b)}</option>`)
      .join("");

    branchSelect.onchange = () => {
      state.dm_selected_branch = branchSelect.value;
      loadCommitsPage();
    };

    document.getElementById("prevPageBtn").onclick = () => {
      const sel = state.dm_selected_branch;
      const page = state.dm_commit_pages[sel] || 1;
      state.dm_commit_pages[sel] = Math.max(1, page - 1);
      loadCommitsPage();
    };
    document.getElementById("nextPageBtn").onclick = () => {
      const sel = state.dm_selected_branch;
      const page = state.dm_commit_pages[sel] || 1;
      state.dm_commit_pages[sel] = page + 1;
      loadCommitsPage();
    };

    loadCommitsPage();
    // Guard: only load file explorer once — it makes expensive API calls
    // (list_files + file_content) and renderAll() fires multiple times.
    if (!state.dm_file_explorer_loaded) {
      state.dm_file_explorer_loaded = true;
      loadFileExplorer();
    }
  }

  async function loadCommitsPage() {
    const gh = state.dm_github;
    const sel = state.dm_selected_branch;
    const page = state.dm_commit_pages[sel] || 1;
    document.getElementById("pageLabel").textContent = `Page: ${page}`;

    const commitsList = document.getElementById("commitsList");
    commitsList.innerHTML = `<div style="font-size:13px;color:#94A3B8;">Loading commits…</div>`;

    let commits = [];
    try {
      const resp = await getJSON("/github/commits", {
        owner: gh.owner, repo: gh.repo, branch: sel, per_page: 30, page: page,
      }, 20000);
      if (resp.status === 200) {
        const data = await resp.json();
        commits = data.commits || [];
      } else {
        // fallback: use all_commits data sliced for this branch/page
        const allBranchCommits = (state.dm_all_commits && state.dm_all_commits.commits_by_branch && state.dm_all_commits.commits_by_branch[sel]) || (gh.commits && gh.commits[sel]) || [];
        commits = allBranchCommits.slice((page - 1) * 30, page * 30);
      }
    } catch (err) {
      const allBranchCommits = (state.dm_all_commits && state.dm_all_commits.commits_by_branch && state.dm_all_commits.commits_by_branch[sel]) || (gh.commits && gh.commits[sel]) || [];
      commits = allBranchCommits.slice((page - 1) * 30, page * 30);
    }

    if (!commits.length) {
      commitsList.innerHTML = `<div style="font-size:13px;color:#94A3B8;">No commits on this page.</div>`;
      return;
    }

    let html = "";
    commits.forEach((c) => {
      const sha = c.sha || "";
      const msg = c.message || (((c.commit || {}).message) || "");
      const author = c.author || (((c.commit || {}).author || {}).name) || "";
      const date = c.date || (((c.commit || {}).author || {}).date || "");
      const shortDate = date ? new Date(date).toLocaleDateString() : "";
      html += `<div style="margin-bottom:14px;">
        <strong style="font-family:'JetBrains Mono',monospace;font-size:12px;">${escapeHtml(sha.slice(0,8))}</strong>
        — ${escapeHtml(msg.split("\n")[0])}
        <span style="color:#94A3B8;font-size:11px;margin-left:8px;">${escapeHtml(author)} ${shortDate}</span>
        <details style="margin-top:4px;" onToggle="if(this.open && !this.dataset.loaded){this.dataset.loaded=1; loadCommitDetail(this, '${escapeHtml(gh.owner)}','${escapeHtml(gh.repo)}','${escapeHtml(sha)}');}">
          <summary style="cursor:pointer;color:#0070F3;font-size:12px;">View changed files</summary>
          <div class="commit-detail-${escapeHtml(sha)}" style="margin-top:6px;font-size:12px;color:#64748B;">Loading…</div>
        </details>
      </div>`;
    });
    commitsList.innerHTML = html;
  }

  async function loadCommitDetail(detailEl, owner, repo, sha) {
    const container = detailEl.querySelector(`[class^="commit-detail-"]`);
    try {
      const resp = await fetch(`${BACKEND_URL}/github/commit/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/${encodeURIComponent(sha)}`, { signal: AbortSignal.timeout(20000) });
      if (!resp.ok) throw new Error(resp.status);
      const data = await resp.json();
      const files = data.files || [];
      if (!files.length) { container.textContent = "No file changes."; return; }
      container.innerHTML = files.slice(0, 10).map(f => `
        <div style="margin-bottom:8px;">
          <span style="color:#334155;">${escapeHtml(f.filename || "")}</span>
          <span style="color:#22c55e;margin-left:6px;">+${f.additions||0}</span>
          <span style="color:#ef4444;margin-left:4px;">-${f.deletions||0}</span>
          ${f.patch ? `<details style="margin-top:4px;"><summary style="cursor:pointer;color:#0070F3;font-size:11px;">Patch</summary><pre style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:8px;font-size:11px;overflow-x:auto;white-space:pre-wrap;">${escapeHtml(f.patch.slice(0,20000))}</pre></details>` : ""}
        </div>`).join("");
    } catch(e) {
      container.textContent = `Failed to load: ${e}`;
    }
  }

  async function loadFileExplorer() {
    const fileSelect = document.getElementById("fileSelect");
    const fileFilterInput = document.getElementById("fileFilterInput");
    const filePreview = document.getElementById("filePreview");

    let fileList = [];
    try {
      // Pass selected branch so list_files uses git trees API recursively
      const resp = await postJSON("/github/list_files", { url: state.dm_repo, per_branch_limit: 100 }, 30000);
      if (resp.status === 200) {
        const data = await resp.json();
        fileList = data.files || [];
      }
    } catch (err) {
      fileList = [];
    }

    function populateOptions(filterText) {
      const filtered = filterText
        ? fileList.filter((p) => p.toLowerCase().includes(filterText.toLowerCase()))
        : fileList;
      const display = filtered.slice(0, 200);
      fileSelect.innerHTML = display.map((p) => `<option value="${escapeHtml(p)}">${escapeHtml(p)}</option>`).join("");
      return display;
    }

    let currentDisplay = populateOptions("");

    fileFilterInput.oninput = () => {
      currentDisplay = populateOptions(fileFilterInput.value);
      if (currentDisplay.length) {
        fileSelect.value = currentDisplay[0];
        loadFilePreview(currentDisplay[0]);
      } else {
        filePreview.innerHTML = "";
      }
    };

    fileSelect.onchange = () => loadFilePreview(fileSelect.value);

    if (currentDisplay.length) {
      fileSelect.value = currentDisplay[0];
      loadFilePreview(currentDisplay[0]);
    } else {
      filePreview.innerHTML = `<div style="font-size:13px;color:#94A3B8;">No files found.</div>`;
    }
  }

  async function loadFilePreview(path) {
    const filePreview = document.getElementById("filePreview");
    if (!path) {
      filePreview.innerHTML = "";
      return;
    }
    filePreview.innerHTML = `<div style="font-size:13px;color:#94A3B8;">Loading file…</div>`;
    let content;
    try {
      const resp = await postJSON("/github/file_content", { url: state.dm_repo, path: path }, 30000);
      if (resp.status === 200) {
        const data = await resp.json();
        content = data.content || "";
      } else {
        content = `Failed to fetch file: ${resp.status}`;
      }
    } catch (err) {
      content = `Error: ${err}`;
    }
    filePreview.innerHTML = `
      <div style="font-size:13px;font-weight:600;margin-bottom:6px;">Preview: ${escapeHtml(path)}</div>
      <pre class="dm-code" style="border-radius:8px;max-height:400px;overflow:auto;">${escapeHtml(content.slice(0, 20000))}</pre>`;
  }

  // ── Form submit ──────────────────────────────────────────────────────────
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    runAnalysis(repoUrlInput.value);
  });

  // Initial render (empty state)
  renderAll();
})();

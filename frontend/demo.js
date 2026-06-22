// ── GitMind demo page wiring ─────────────────────────────────────────────
// Mirrors the logic of the original Streamlit demo page (pages/req_demo.py,
// now removed from this repo) exactly: same endpoints, same payloads,
// same fallback-to-demo-mode behavior, same mock data.

(function () {
  const BACKEND_URL = window.BACKEND_URL || "http://localhost:8000";

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

  // ── STATE (mirrors st.session_state) ────────────────────────────────────
  const state = {
    dm_loading: false,
    dm_loaded: false,
    dm_result: null,
    dm_error: null,
    dm_repo: "",
    dm_github: null,
    dm_all_commits: null,
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

  // ── Main analyze flow (mirrors `if analyze:` block) ────────────────────
  async function runAnalysis(rawUrl) {
    state.dm_loading = true;
    state.dm_loaded = false;
    state.dm_result = null;
    state.dm_error = null;
    state.dm_repo = (rawUrl || "").trim() || "https://github.com/demo/sample-repo";

    renderLoading();

    const queryText = `why did the repository ${state.dm_repo} fail`;

    // POST /query
    try {
      const resp = await postJSON("/query", { query: queryText }, 30000);
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

    // POST /github/fetch for repo metadata + /github/all_commits for full history
    try {
      const ghResp = await postJSON("/github/fetch", { url: state.dm_repo, per_branch_limit: 30 }, 60000);
      if (ghResp.status === 200) {
        state.dm_github = await ghResp.json();
        state.dm_selected_branch = state.dm_github.default_branch ||
          (state.dm_github.branches && state.dm_github.branches[0]) || "";
        state.dm_commit_pages = {};
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
        <div class="dm-loading-text">Building causal dependency graph…</div>
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
      statsBox.innerHTML = `
        <div class="dm-stats-grid">
          <div class="dm-stat-box"><div class="dm-stat-num" style="color:#003D6B;">47</div><div class="dm-stat-label">Commits</div></div>
          <div class="dm-stat-box"><div class="dm-stat-num" style="color:#003D6B;">12</div><div class="dm-stat-label">Tickets</div></div>
          <div class="dm-stat-box"><div class="dm-stat-num" style="color:#0070F3;">2</div><div class="dm-stat-label">Issues</div></div>
          <div class="dm-stat-box"><div class="dm-stat-num" style="color:#10B981;">0</div><div class="dm-stat-label">Regressions</div></div>
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

  function renderGraph() {
    const chain = (state.dm_result && state.dm_result.causal_chain) || [];
    if (chain.length > 0) {
      // No pyvis in-browser equivalent bundled; render a simple node list
      // summarizing the causal chain (keeps wiring/data identical; visual
      // network rendering can be swapped for a JS graph lib if desired).
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
      if (state.dm_error === "demo" || result == null) {
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
      } else if (result) {
        const chain = result.causal_chain || [];
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
        body = items
          ? `<div class="dm-timeline">${items}</div>`
          : `<div class="dm-empty"><div class="dm-empty-icon">&#9201;</div><div class="dm-empty-title">No timeline data</div><div class="dm-empty-desc">No causal nodes returned from backend.</div></div>`;
      } else {
        body = `<div class="dm-empty"><div class="dm-empty-icon">&#9201;</div><div class="dm-empty-title">Timeline empty</div><div class="dm-empty-desc">Commit history and causal links will appear here after analysis.</div></div>`;
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
      repoExplorer.style.display = "none";
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
    loadFileExplorer();
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
      const msg = c.message || "";
      html += `<div style="margin-bottom:14px;"><strong style="font-family:'JetBrains Mono',monospace;font-size:12px;">${escapeHtml(sha.slice(0,8))}</strong> — ${escapeHtml(msg)}`;
      const files = c.files || [];
      files.slice(0, 5).forEach((f) => {
        html += `
          <div style="margin-top:4px;font-size:12px;color:#64748B;">
            ${escapeHtml(f.filename || "")} — +${f.additions || 0} -${f.deletions || 0}
            ${f.patch ? `<details style="margin-top:4px;"><summary style="cursor:pointer;color:#0070F3;font-size:12px;">Patch: ${escapeHtml(f.filename || "")}</summary><pre style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:8px;font-size:11px;overflow-x:auto;white-space:pre-wrap;">${escapeHtml(f.patch.slice(0, 20000))}</pre></details>` : ""}
          </div>`;
      });
      html += `</div>`;
    });
    commitsList.innerHTML = html;
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

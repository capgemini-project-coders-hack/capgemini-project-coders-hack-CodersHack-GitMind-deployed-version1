// ── GitMind landing page wiring ──────────────────────────────────────────
// Mirrors the logic of the original Streamlit landing page (now removed
// from this repo) exactly:
//  - "Request Early Access" / "See How It Works" buttons → smooth scroll
//  - Waitlist form → validate, dedupe (in-memory, mirrors st.session_state),
//    POST to {BACKEND_URL}/waitlist, show success/error/warning message.

(function () {
  const BACKEND_URL = window.BACKEND_URL || "http://localhost:8000";

  // In-memory waitlist mirror of Streamlit's st.session_state.waitlist.
  // NOTE: this resets on page reload, same as Streamlit session_state would
  // reset on a fresh session. The backend /waitlist call is fire-and-forget
  // (same as the original: requests.post(..., timeout=3) wrapped in try/except).
  const waitlist = [];

  // Hero buttons → smooth scroll to section (mirrors components.html scrollIntoView)
  const heroAccessBtn = document.getElementById("heroAccessBtn");
  const heroHiwBtn = document.getElementById("heroHiwBtn");

  if (heroAccessBtn) {
    heroAccessBtn.addEventListener("click", () => {
      const el = document.getElementById("get-access");
      if (el) el.scrollIntoView({ behavior: "smooth" });
    });
  }
  if (heroHiwBtn) {
    heroHiwBtn.addEventListener("click", () => {
      const el = document.getElementById("how-it-works");
      if (el) el.scrollIntoView({ behavior: "smooth" });
    });
  }

  // Waitlist form
  const form = document.getElementById("waitlistForm");
  const emailInput = document.getElementById("emailInput");
  const formMsg = document.getElementById("formMsg");
  const countEl = document.getElementById("waitlistCount");

  const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

  function setMsg(text, kind) {
    formMsg.textContent = text;
    formMsg.className = "gm-form-msg" + (kind ? " " + kind : "");
  }

  function updateCount() {
    if (waitlist.length > 0) {
      countEl.textContent = `${waitlist.length} engineers on the waitlist`;
    } else {
      countEl.textContent = "";
    }
  }

  if (form) {
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      const email = (emailInput.value || "").trim();

      if (!email) {
        setMsg("Please enter your email address.", "error");
        return;
      }
      if (!EMAIL_RE.test(email)) {
        setMsg("Please enter a valid email address.", "error");
        return;
      }
      if (waitlist.includes(email)) {
        setMsg("This email is already registered.", "warning");
        return;
      }

      waitlist.push(email);

      // Fire-and-forget POST to backend, same as original (timeout, swallow errors)
      try {
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), 3000);
        await fetch(`${BACKEND_URL}/waitlist`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email: email, ts: new Date().toISOString() }),
          signal: controller.signal,
        });
        clearTimeout(t);
      } catch (err) {
        // swallow, same as original `except Exception: pass`
      }

      setMsg("You're on the list. We'll be in touch.", "success");
      updateCount();
      emailInput.value = "";
    });
  }
})();

/* theme.js — GitMind dark/light mode toggle
   Default: light mode. Persists choice in localStorage and applies
   data-theme="dark" on <html> when dark mode is active. */
(function () {
  var STORAGE_KEY = "gitmind-theme";

  function getSavedTheme() {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }

  function saveTheme(theme) {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {
      /* ignore — private browsing / storage disabled */
    }
  }

  function applyTheme(theme) {
    if (theme === "dark") {
      document.documentElement.setAttribute("data-theme", "dark");
    } else {
      document.documentElement.removeAttribute("data-theme");
    }
  }

  function initThemeToggle() {
    var toggle = document.getElementById("themeToggle");
    if (!toggle) return;

    var current = getSavedTheme() === "dark" ? "dark" : "light";
    applyTheme(current);
    toggle.setAttribute("aria-pressed", current === "dark" ? "true" : "false");

    toggle.addEventListener("click", function () {
      current = current === "dark" ? "light" : "dark";
      applyTheme(current);
      saveTheme(current);
      toggle.setAttribute("aria-pressed", current === "dark" ? "true" : "false");
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initThemeToggle);
  } else {
    initThemeToggle();
  }
})();

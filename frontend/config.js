// Backend base URL.
// Requests go to /api/... which Vercel rewrites to the Render backend.
// This eliminates CORS entirely — same-origin requests don't need CORS headers.
// For local dev, set window.BACKEND_URL = "http://localhost:8000" before this script.
window.BACKEND_URL = window.BACKEND_URL || "";

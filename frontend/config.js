// Backend base URL.
// Streamlit used: os.getenv("BACKEND_URL", "http://localhost:8000")
// On Vercel there's no server-side env injection for static files, so:
//   1. Edit BACKEND_URL below to your deployed FastAPI backend URL, OR
//   2. Leave as-is for local dev (backend on localhost:8000).
// Make sure CORS_ALLOWED_ORIGINS on the backend includes your Vercel domain.
window.BACKEND_URL = "https://gitmind-backend-pgv8.onrender.com";

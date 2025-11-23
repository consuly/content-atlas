# Repository Guidelines

## Data Integrity First
- Every workflow must maximize import fidelity; treat mapping completeness and correctness as the top priority.
- All data ingestion, transformation, and export paths must surface errors and duplicates clearly so users can resolve them; no silent drops.
- Design processors, schemas, and UI flows to help users reach a 100% success rate, including actionable feedback for fixes.
- Add tests and fixtures that prove mappings remain complete and that failures are reported and recoverable.

## Project Structure & Module Organization
- FastAPI backend lives in `app/`, with routers under `app/routers/`, domain logic in `app/processors/`, and shared schemas in `app/schemas.py`.
- Data fixtures and pytest suites live in `tests/`; keep new fixtures alongside the feature they support.
- Frontend is a Refine + Vite app in `frontend/` (`src/` for React views, `public/` for static assets).
- Operational scripts (`*.py` helpers, Dockerfile, `docker-compose.yml`) sit at the repo root, and detailed references live in `docs/`.

## Build, Test, and Development Commands
- `python -m pip install -r requirements.txt` installs backend dependencies.
- `uvicorn app.main:app --reload` runs the API with live reload; ensure Postgres is available.
- `python -m pytest` executes the backend suite (see `docs/TESTING.md` for advanced flags).
- `docker-compose up -d` starts the database and optional services used by local dev.
- Frontend: `cd frontend && npm install` once, then `npm run dev` for hot reload and `npm run build` for production bundles.

## Coding Style & Naming Conventions
- Python follows 4-space indentation, type hints for new interfaces, and `snake_case` for functions/variables; expose routers via module-level `APIRouter` instances mirroring current names.
- Keep pydantic models and SQLAlchemy models in their existing modules (`app/schemas.py`, `app/models.py`) to avoid import cycles.
- React/TypeScript components use functional components with `PascalCase` filenames; hooks and helpers stay `camelCase`. Align with `frontend/eslint.config.js` when adding lint rules.

## Testing Guidelines
- Use pytest with `test_*.py` modules and function-level assertions; leverage existing fixtures in `tests/conftest.py`.
- Prefer targeted data under `tests/csv/` or create new files in nested directories to avoid polluting root fixtures.
- Aim to cover new data flows, especially around mapping and duplicate detection; update `docs/TESTING.md` if workflows change.

## Commit & Pull Request Guidelines
- Recent history shows short, imperative subject lines (e.g., “Fix mapping error…”). Keep subjects under ~72 characters and describe the outcome, not the implementation.
- Every PR should summarize impact, note schema changes, and link the tracking issue or ticket.
- Include verification steps (pytest, frontend build, Docker if relevant) in the PR description, and attach UI screenshots when modifying `frontend/src/`.

## Security & Configuration Tips
- Store secrets such as `DATABASE_URL` and B2 keys in a local `.env`; never commit them.
- When sharing configs, scrub sample files and reference `docs/DEPLOYMENT.md` for production hardening steps.

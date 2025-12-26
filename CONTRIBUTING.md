# Contributing to ContentAtlas

First off, thanks for taking the time to contribute! ContentAtlas is a data consolidation platform built with Python (FastAPI), React, and PostgreSQL, leveraging LLMs for natural language querying.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Developing with AI Assistance](#developing-with-ai-assistance)
- [Pull Request Process](#pull-request-process)

---

## Getting Started

Please refer to the following guides to set up your environment:

1.  **[README.md](README.md)**: General project overview and quick start.
2.  **[docs/SETUP.md](docs/SETUP.md)**: Detailed environment setup and configuration.
3.  **[docs/FRONTEND_SETUP.md](docs/FRONTEND_SETUP.md)**: Specific instructions for the React frontend.

**Prerequisites:**
- Python 3.8+
- Node.js & npm
- PostgreSQL 12+ (or Docker)
- Docker (optional but recommended for DB)

---

## Development Workflow

1.  **Branching**: Use feature branches for all changes.
    *   `feature/my-new-feature`
    *   `fix/bug-description`
2.  **Commits**: Use imperative mood for commit messages.
    *   Good: "Add duplicate detection for CSV files"
    *   Bad: "Added duplicate detection" or "Fixing bugs"
3.  **Keep it Small**: Try to keep pull requests focused on a single feature or fix.

---

## Coding Standards

Our codebase follows strict guidelines to ensure maintainability and AI-readability.

### General
*   **File Size**: Keep files under **500 lines**. If a file grows larger, split it into logical modules.
*   **Naming**:
    *   Python: `snake_case` for functions/variables.
    *   React: `PascalCase` for components, `camelCase` for hooks/helpers.
    *   Files: `snake_case.py` (Python), `PascalCase.tsx` (React components).

### Backend (Python/FastAPI)
*   **Type Hints**: Use type hints for all new interfaces and functions.
*   **Pydantic**: Use Pydantic models (`app/schemas.py`) for all API request/response schemas.
*   **Async/Await**:
    *   Use `async` for I/O-bound operations (DB queries, file uploads).
    *   Use synchronous code for CPU-bound tasks (Pandas data transformations).
*   **Structure**:
    *   `app/routers/`: API endpoints.
    *   `app/processors/`: Domain logic and file processing strategies.
    *   `app/schemas/`: Shared data models.

### Frontend (React/Vite)
*   **Functional Components**: Use React functional components with TypeScript.
*   **Refine Framework**: We use [Refine](https://refine.dev/) for internal tooling interfaces.
*   **Linting**: Follow the rules in `frontend/eslint.config.js`.

### Architecture Patterns
*   **Data Processing**: Use the Strategy Pattern for file processors.
*   **Consolidation**: Implement two-phase processing (Duplicate Check -> Insert).
*   **Validation**: Validate data types during mapping, not just at insertion.

---

## Testing

We prioritize test coverage to ensure stability.

*   **Backend**: We use `pytest`.
    *   Run all tests: `pytest`
    *   Run specific test: `pytest tests/test_api.py`
    *   See **[docs/TESTING.md](docs/TESTING.md)** for detailed testing workflows.
*   **Frontend**: Manual testing via `npm run dev`.
*   **New Features**: Every new feature **must** include accompanying tests.
*   **Data Integrity**: Add tests for new mapping logic to ensure no silent data drops.

---

## Developing with AI Assistance

We actively use LLMs (Large Language Models) to build and maintain this project. We encourage contributors to use AI tools to improve productivity and code quality.

### Recommended Tools
*   **Cline / Cursor**: We use AI coding assistants integrated into VS Code.
*   **LLMs**: Claude 3.5 Sonnet (recommended for coding tasks), GPT-4o.

### Context is King
To get the best results from AI, you must provide it with the right context. This repository is structured to help AI agents understand the codebase:

1.  **`.clinerules/`**: Contains project-specific rules and architectural overviews. If using Cline, this is automatically loaded.
2.  **`AGENTS.md`**: Found in the root and subdirectories. These files contain specific context and guidelines for that part of the codebase. **Always ask your AI to read the relevant `AGENTS.md` file.**
3.  **`docs/`**: Our documentation is comprehensive. Before asking an AI to implement a feature, ask it to read the relevant documentation (e.g., `docs/DUPLICATE_DETECTION.md` before working on imports).

### Workflow for AI-Assisted Dev
1.  **Prompting**: Start by explaining the task and telling the AI to read the project structure (`README.md`, `AGENTS.md`).
2.  **Planning**: Ask the AI to "Plan" or "Analyze" before writing code.
3.  **TDD**: Ask the AI to write the tests *before* or *alongside* the implementation.
4.  **Review**: AI makes mistakes. Always review the code against the [Coding Standards](#coding-standards) above. specifically checking for:
    *   Correct usage of Pandas/SQLAlchemy.
    *   Proper error handling (no silent failures).
    *   Adherence to file size limits.

---

## Pull Request Process

1.  Ensure your code builds and runs locally.
2.  Run the tests: `pytest`.
3.  Update the documentation if you changed any behavior or APIs.
4.  Submit your PR with a clear description of the changes and a link to the issue it resolves.

Happy Coding! 🚀

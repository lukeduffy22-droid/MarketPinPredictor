# Copilot instructions for MarketPinPredictor

## Project context
- Python market-index prediction system with:
  - FastAPI backend (`app/api/main.py`, launched via `server.py`); `/health` is the smoke check.
  - Streamlit dashboard (`app.py`) for visualization.
- Core gamma-exposure logic and invariants live in `app/core/gex.py` and are validated by tests in `tests/test_gex.py` and `tests/test_gex_invariants.py`. This module is the single source of truth for GEX formulas/sign conventions.

## Technology stack
- **Python**: >=3.9
- **Backend**: FastAPI, Uvicorn
- **Frontend**: Streamlit
- **Data processing**: NumPy, Pandas, SciPy
- **Machine Learning**: scikit-learn, XGBoost, TensorFlow, PyTorch
- **Visualization**: Plotly
- **Market data**: Polygon.io API client
- **Database**: PostgreSQL with SQLAlchemy
- **Testing**: pytest
- **Package manager**: uv or pip

## Project structure
```
MarketPinPredictor/
├── app/
│   ├── api/          # FastAPI backend
│   │   └── main.py   # Main API application
│   └── core/
│       └── gex.py    # Core gamma-exposure calculations (single source of truth)
├── tests/
│   ├── test_gex.py              # GEX functionality tests
│   └── test_gex_invariants.py   # GEX invariant validation
├── server.py         # FastAPI server launcher
├── app.py            # Streamlit dashboard
└── pyproject.toml    # Project dependencies
```

## Code changes
- **Keep changes minimal and localized** to the specific issue; avoid drive-by refactors.
- **Do not alter GEX sign conventions or invariants**; reuse `app/core/gex.py` helpers instead of duplicating logic.
- **Preserve existing API contracts** and data shapes for both FastAPI and Streamlit flows.
- **Reuse existing utilities** under `app/` before adding new modules or dependencies.
- **Follow existing code style** in the files you're modifying.
- **Add tests** for new functionality or bug fixes when appropriate.

## Coding standards
- **Type hints**: Use type annotations for function parameters and return values
- **Docstrings**: Add docstrings for public functions and classes
- **Error handling**: Use appropriate exceptions and error messages
- **Testing**: Write tests that validate both happy path and edge cases
- **Imports**: Group imports (stdlib, third-party, local) with blank lines between groups
- **Naming**: Use descriptive variable names; follow PEP 8 conventions

## Development setup
- Python 3.9+; install deps with `pip install -r requirements_local.txt` (fast path) or `uv pip install .` (full env).
- Avoid adding new dependencies unless required for the task.
- Keep secrets, API keys, and large artifacts out of the repo.

## Build and test commands

### Running tests
```bash
# Run targeted GEX tests first
python -m pytest tests/test_gex.py -q
python -m pytest tests/test_gex_invariants.py -q

# Run all tests
pytest -v
```

### Running the application
```bash
# Start FastAPI backend
python server.py
# Or directly with uvicorn
python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8000

# Start Streamlit dashboard (in a separate terminal)
streamlit run app.py
```

### Validation commands
- **Run targeted tests first** before making changes to understand baseline behavior
- **For GEX changes**: Run `python -m pytest tests/test_gex.py tests/test_gex_invariants.py -q`
- **For backend changes**: Verify app startup and check `GET /health` endpoint returns JSON
- **For all changes**: Ensure existing tests still pass after your modifications

## Common workflows

### Modifying GEX calculations
1. Review `app/core/gex.py` and understand current logic
2. Check `tests/test_gex_invariants.py` for invariants that must be preserved
3. Make changes while preserving mathematical invariants
4. Run both GEX test files to validate

### Fixing a bug
1. Write a test that reproduces the bug
2. Make the minimal fix in the relevant module
3. Verify the test now passes
4. Run related tests to ensure no side effects

## Copilot task execution guidance (to avoid VS Code chat task failures)
- **Break work into small, file-scoped tasks**: Example: "update `app/core/gex.py` and `tests/test_gex.py` only"
- **Include explicit acceptance criteria**: Specify expected behavior, tests to run, and files allowed to change
- **Ask for a plan first**: Request a short plan before implementation
- **Retry with narrower scope**: If chat returns an error code, rerun with a narrower prompt and fewer files in scope
- **Validate incrementally**: Test each change before moving to the next step

## Security and best practices
- **Never commit secrets**: Use environment variables for API keys and credentials
- **Validate inputs**: Always validate user inputs, especially in API endpoints
- **Handle errors gracefully**: Provide meaningful error messages without exposing sensitive information
- **Test edge cases**: Consider boundary conditions and error scenarios in tests

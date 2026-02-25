# Copilot instructions for MarketPinPredictor

## Project overview
This repository contains a Python-based market index prediction system focused on gamma exposure (GEX) analysis. The system provides:
- **FastAPI backend** (`app/api/main.py`, launched via `server.py`) for API services
- **Streamlit dashboards** (`app.py`, `main.py`) for interactive visualization
- **Core gamma-exposure logic** in `app/core/gex.py` with comprehensive test coverage

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
│       └── gex.py    # Core gamma-exposure calculations
├── tests/
│   ├── test_gex.py              # GEX functionality tests
│   └── test_gex_invariants.py   # GEX invariant validation
├── server.py         # FastAPI server launcher
├── app.py            # Main Streamlit dashboard
├── main.py           # Alternative Streamlit entry point
└── pyproject.toml    # Project dependencies
```

## Code changes
- **Keep changes minimal and localized** to the specific issue
- **Do not refactor unrelated modules** while fixing a targeted issue
- **Preserve existing API contracts** and GEX sign/invariant behavior
- **Reuse existing utilities** under `app/` before adding new modules
- **Follow existing code style** in the files you're modifying
- **Add tests** for new functionality or bug fixes when appropriate

## Coding standards
- **Type hints**: Use type annotations for function parameters and return values
- **Docstrings**: Add docstrings for public functions and classes
- **Error handling**: Use appropriate exceptions and error messages
- **Testing**: Write tests that validate both happy path and edge cases
- **Imports**: Group imports (stdlib, third-party, local) with blank lines between groups
- **Naming**: Use descriptive variable names; follow PEP 8 conventions

## Build and test commands

### Environment setup
```bash
# Using uv (recommended)
uv pip install .

# Or using pip
pip install -r requirements_local.txt
```

### Running tests
```bash
# Run all tests
pytest

# Run specific test files
python -m pytest tests/test_gex.py -q
python -m pytest tests/test_gex_invariants.py -q

# Run with verbose output
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

### Adding a new feature
1. Review related code in `app/` to understand existing patterns
2. Write tests first (TDD approach) in `tests/`
3. Implement the feature, reusing existing utilities
4. Run targeted tests to validate
5. Run full test suite to ensure no regressions

### Fixing a bug
1. Write a test that reproduces the bug
2. Make the minimal fix in the relevant module
3. Verify the test now passes
4. Run related tests to ensure no side effects

### Modifying GEX calculations
1. Review `app/core/gex.py` and understand current logic
2. Check `tests/test_gex_invariants.py` for invariants that must be preserved
3. Make changes while preserving mathematical invariants
4. Run both GEX test files to validate

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

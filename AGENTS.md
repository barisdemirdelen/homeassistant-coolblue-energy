# AGENTS.md

Home Assistant custom integration (`custom_components/coolblue_energy`, domain `coolblue_energy`).
Python 3.14 (`.python-version`), dependencies managed by **uv** (`pyproject.toml` + `uv.lock`).

## Toolchain

Everything runs through `uv`. **Never** invoke `python`, `pip`, `pytest`, `ruff`, or `ty`
directly — they resolve outside the project venv. Never activate `.venv` manually.

| Task            | Command                     |
| --------------- | --------------------------- |
| Sync env        | `uv sync`                   |
| Add runtime dep | `uv add <pkg>`              |
| Add dev dep     | `uv add --dev <pkg>`        |
| Run anything    | `uv run <cmd>`              |
| Lint            | `uv run ruff check .`       |
| Autofix lint    | `uv run ruff check --fix .` |
| Format          | `uv run ruff format <path>` |
| Type check      | `uv run ty check`           |
| Tests           | `uv run pytest`             |

`uv add` edits `pyproject.toml` and `uv.lock` — do not hand-edit either. Runtime deps that
Home Assistant must install at load time also belong in
`custom_components/coolblue_energy/manifest.json` under `requirements`.

## Before you hand work back

Run, in this order, and fix what you broke:

```
uv run ruff check .
uv run ty check
uv run pytest
```

Notes:

- **`ruff`** — no `[tool.ruff]` config exists; defaults apply. The repo is not clean at
  baseline (`tests/test_coordinator.py` has pre-existing `F401`s). Judge yourself on
  *new* diagnostics, not the total count.
- **`ruff format`** — the repo is *not* uniformly formatted (6 files would be reformatted).
  Format only files you touched: `uv run ruff format path/to/file.py`. Never run
  `uv run ruff format .` — it produces a large unrelated diff.
- **`ty`** — Astral's type checker, currently green across the repo. Keep it green; a `ty`
  error in code you touched is a blocker.
- **`pytest`** — 135 tests, ~4s. Config in `pytest.ini`: `testpaths = tests`,
  `asyncio_mode = auto` (async tests need no `@pytest.mark.asyncio`). Narrow with
  `uv run pytest tests/test_coordinator.py -k some_case`. Coverage via `pytest-cov`:
  `uv run pytest --cov=custom_components/coolblue_energy`.

## Tests / TDD

Default to test-first for features and bug fixes: **red → green**, one vertical slice at a
time. Write one failing test, run it, then write only enough code to pass it — never bulk out
a suite against imagined behavior.

```
uv run pytest tests/test_coordinator.py -k my_new_case   # red: confirm it fails
# ...implement...
uv run pytest tests/test_coordinator.py -k my_new_case   # green
uv run pytest                                            # full suite, ~4s
```

For a bug fix, the failing test must reproduce the reported bug *before* the fix — a test
written after the fix proves nothing.

Test at the existing seams, one file per seam:

| Seam                                | Test file                  |
| ----------------------------------- | -------------------------- |
| `api_client.ApiClient` (HTTP/parse)  | `tests/test_api_client.py` |
| `coordinator` (fetch → statistics)   | `tests/test_coordinator.py`|
| `config_flow` (setup UI)             | `tests/test_config_flow.py`|

Assert on observable behavior at those boundaries, not on private helpers or internal call
order. Fixtures and entry factories live in `tests/conftest.py` (`make_electricity_entry`,
`make_day_gas`, `mock_api_client`, `mock_hass`, `coordinator`, …) — build test data from
those helpers instead of hand-rolling `MeterReadingEntry` literals, and add new factories
there rather than duplicating setup per test.

## CI

GitHub Actions only runs hassfest validation, HACS validation, and the release packaging
workflow. **No workflow runs ruff, ty, or pytest** — local checks are the only gate, so run
them.

## Versioning

Version bumps are manual and must land in **both** places, in the same commit, with the same
value: `version` in `pyproject.toml` and `version` in
`custom_components/coolblue_energy/manifest.json` (both currently `1.1.2`). The release
workflow re-stamps `manifest.json` from the GitHub release tag and **fails the release** if
the committed value doesn't match, so a bump that misses one file breaks publishing.

## Housekeeping

`scratch.py` is gitignored throwaway scratch space — don't treat it as source, don't lint or
fix it. `.venv/`, `.ruff_cache/`, `.pytest_cache/`, `__pycache__/` are all ignored.

# AGENTS.md

Home Assistant custom integration (`custom_components/coolblue_energy`, domain `coolblue_energy`).
Python 3.14 (`.python-version`), floor `>=3.14.2` in `pyproject.toml` because that is what
`homeassistant` itself requires. Dependencies managed by **uv** (`pyproject.toml` + `uv.lock`).

> **Py3.14 syntax quirk — do not "fix" it.** `except RuntimeError, ValueError:` (no
> parentheses) in `api_client.py::_retry_with_backoff` is *valid* under PEP 758, new in 3.14.
> It is identical to the Python 2 form that was a `SyntaxError` in 3.0–3.13, so it reads like a
> bug. The bare form is allowed only without an `as` clause; with a binding you still need
> `except (A, B) as exc:`. Confirm with `ast.parse` before touching suspicious syntax here.

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

- **`ruff`** — pinned to `>=0.16` in the dev group. Ruff 0.16 expanded its *default* rule set
  from 59 rules (`E4,E7,E9,F`) to 413 (adds `UP`, `SIM`, `DTZ`, `BLE`, `PLR`, `RET`, `RUF`,
  `PYI`, `C4`, `YTT`, …). There is deliberately no `[tool.ruff]` config — defaults apply, and
  the repo is **clean at baseline**. Any diagnostic is yours; fix it rather than adding
  `# noqa` or a config exemption.
- **`ruff format`** — the repo *is* uniformly formatted, and `.` covers more than `.py`: ruff
  also formats Python fenced blocks inside Markdown, so `docs/**.md` is in scope and a doc
  being reformatted is expected, not collateral damage. Format what you touched
  (`uv run ruff format path/to/file`) and keep `uv run ruff format --check .` green.
- **dates** — never call `date.today()` / `datetime.now()` bare (`DTZ` rules). Use
  `homeassistant.util.dt` (`from homeassistant.util import dt as dt_util`), e.g.
  `dt_util.now().date()`, so the HA-configured timezone is respected. This applies in tests too.
- **`ty`** — Astral's type checker, currently green across the repo. Keep it green; a `ty`
  error in code you touched is a blocker.
- **`pytest`** — whole suite runs in seconds. Config in `pytest.ini`: `testpaths = tests`,
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
uv run pytest                                            # full suite
```

For a bug fix, the failing test must reproduce the reported bug *before* the fix — a test
written after the fix proves nothing.

Test at the existing seams, one file per seam:

| Seam                                  | Test file                    |
| ------------------------------------- | ---------------------------- |
| `api_client.ApiClient` (HTTP/parse)   | `tests/test_api_client.py`   |
| `coordinator` (fetch → statistics)    | `tests/test_coordinator.py`  |
| `config_flow` (setup UI)              | `tests/test_config_flow.py`  |
| config entry lifecycle (real HA)      | `tests/test_config_entry.py` |

Assert on observable behavior at those boundaries, not on private helpers or internal call
order. Fixtures and entry factories live in `tests/conftest.py` (`make_electricity_entry`,
`make_day_gas`, `mock_api_client`, `coordinator`, …) — build test data from
those helpers instead of hand-rolling `MeterReadingEntry` literals, and add new factories
there rather than duplicating setup per test.

### Two harnesses, temporarily

`tests/conftest.py` hands out a **real** Home Assistant via
`pytest-homeassistant-custom-component` (dev-group only — never add it to `manifest.json`, it
must not reach a user's install). The `coordinator` fixture is a real `CoolblueCoordinator`
on a real `hass` with a real recorder behind it, so `get_instance(hass)` resolves to
something that actually queries statistics.

What remains on hand-rolled mocks is `tests/ha_external_statistics/`, which builds local
`MagicMock` hass objects per test and patches `get_instance` itself. Those are being migrated
away; the real harness is where new tests go.

One rule when writing against it: request `recorder_mock` **before**
`enable_custom_integrations` in the test signature. The integration declares a `recorder`
dependency and the plugin asserts this ordering.

Spy on a real coordinator method rather than replacing it — `_spy_on` in
`tests/test_coordinator.py` wraps the bound method so the call still does its work while the
test counts it.

The plugin pins an exact Home Assistant version, so `homeassistant` and
`pytest-homeassistant-custom-component` must be bumped in lockstep.

## CI

GitHub Actions only runs hassfest validation, HACS validation, and the release packaging
workflow. **No workflow runs ruff, ty, or pytest** — local checks are the only gate, so run
them.

## Versioning

Version bumps are manual and must land in **both** places, in the same commit, with the same
value: `version` in `pyproject.toml` and `version` in
`custom_components/coolblue_energy/manifest.json`. The release workflow re-stamps
`manifest.json` from the GitHub release tag and **fails the release** if
the committed value doesn't match, so a bump that misses one file breaks publishing.

## Housekeeping

`scratch.py` is gitignored throwaway scratch space — don't treat it as source, don't lint or
fix it. `.venv/`, `.ruff_cache/`, `.pytest_cache/`, `__pycache__/` are all ignored.

## Agent skills

### Issue tracker

GitHub Issues on `barisdemirdelen/homeassistant-coolblue-energy`, via the `gh` CLI. See
`docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, each label string equal to its name. See
`docs/agents/triage-labels.md`.

### Domain docs

Single-context — `CONTEXT.md` (glossary) and `docs/adr/` at the repo root, both of which
exist. See `docs/agents/domain.md`.

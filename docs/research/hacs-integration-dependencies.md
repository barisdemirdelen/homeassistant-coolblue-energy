# How a HACS custom integration depends on an external Python package

**Question.** How does a HACS custom integration depend on an external Python package, and
what does that mechanism constrain? Trace it through actual Home Assistant core code, not
documentation summaries.

**Why we asked.** This gates a planning effort to extract `ha_external_statistics/` — currently
vendored inside both `homeassistant-coolblue-energy` and `homeassistant-greenchoice` — into a
shared library. Before choosing "publish it on PyPI" over "keep vendoring" or some other
mechanism, we need the actual constraints core imposes on custom-integration requirements: when
they install, whether they're validated at all, what happens on conflict, and what real HACS
integrations do today. The question is unconditionally load-bearing: it applies equally if we
instead pull in a third-party library.

---

## Method and provenance

GitHub's code-search index is partial (a known limitation noted in the existing
`backfilling-external-statistics.md` note), so for `home-assistant/core` the relevant files were
fetched individually from `raw.githubusercontent.com` at a pinned commit rather than trusted to
search results, and cross-checked against the GitHub API for that same commit.

- **Pinned commit**: [`07a2383bdc4f1a71b0b695e24cbc0c1da823f99b`](https://github.com/home-assistant/core/commit/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b),
  `dev` HEAD as of **2026-07-28**, verified via `git ls-remote https://github.com/home-assistant/core.git refs/heads/dev`
  (fetched 2026-07-28, returned this exact SHA). This is the same commit
  `docs/research/backfilling-external-statistics.md` pinned earlier the same day, confirmed there
  against the GitHub API as `2026.8.0.dev0` (`dev`, committed 2026-07-28T18:47:51Z). Every core
  file quoted below was fetched as `https://raw.githubusercontent.com/home-assistant/core/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/<path>`,
  which GitHub serves byte-for-byte from that exact tree object — content is pinned by
  construction, not by post-hoc hashing.
- Files read in full or by section: `homeassistant/requirements.py`, `homeassistant/util/package.py`,
  `homeassistant/loader.py` (targeted greps + line ranges), `homeassistant/const.py`,
  `homeassistant/package_constraints.txt`, `pyproject.toml`, `script/hassfest/requirements.py`,
  `script/hassfest/manifest.py`.
- **HACS integration source**: `https://github.com/hacs/integration`, `main` branch, files fetched
  2026-07-28 via `raw.githubusercontent.com` (unpinned — HACS ships no version tags on `main` for
  this comparison; behavior confirmed stable against the files actually being read, e.g.
  `custom_components/hacs/utils/validate.py`, `custom_components/hacs/validate/integration_manifest.py`,
  `custom_components/hacs/repositories/integration.py`, `custom_components/hacs/manifest.json`).
- **Official docs**: `https://developers.home-assistant.io/docs/creating_integration_manifest/`
  (fetched 2026-07-28, Markdown content negotiation), `https://docs.pypi.org/trusted-publishers/`
  (fetched 2026-07-28), `https://www.home-assistant.io/blog/2025/05/22/deprecating-core-and-supervised-installation-methods-and-32-bit-systems/`
  (fetched 2026-07-28), `https://developers.home-assistant.io/docs/operating-system/partition/`
  (via web search citation, corroborated by `deepwiki.com/home-assistant/operating-system`
  mirrors of the same developer docs).
- **PyPI JSON API**: `https://pypi.org/pypi/<project>/json`, fetched 2026-07-28, for `ha-garmin`,
  `alexapy`, `homeassistant-historical-sensor`, `garminconnect`.
- **Precedent manifests**: fetched raw from each integration's GitHub repo, `2026-07-28`:
  `alandtse/alexa_media_player` (`dev` branch), `cyberjunky/home-assistant-garmin_connect`
  (`master`), `sebr/bhyve-home-assistant` (`master`), `hacs/integration` (`main`), plus this repo's
  own `custom_components/coolblue_energy/manifest.json` and
  `/home/burger/Projects/homeassistant-greenchoice/custom_components/greenchoice/manifest.json`
  and both repos' `hacs.json`, read directly off disk.
- Greps used against the fetched `loader.py`: `is_built_in|custom_components|def requirements|CUSTOM|requirements: list\[str\]|_load_available_integrations|async_get_custom_components`
  — this is how the custom-vs-built-in distinction below (`PACKAGE_CUSTOM_COMPONENTS`,
  `_get_custom_components`, `Integration.is_built_in`, `Integration.core`) was located rather than
  guessed.

---

## Answer

1. **Requirements install through one shared, non-custom-aware pip/uv path — the only
   distinction is a warning, a quality-scale label, and a stricter format check.** Every
   integration, built-in or custom, funnels through
   `RequirementsManager.async_process_requirements` in `homeassistant/requirements.py`, which
   calls `uv pip install` via `homeassistant/util/package.py:install_package`. Custom integrations
   are marked (`integration.is_built_in` is `False`), get a one-time `CUSTOM_WARNING` log line and
   default to `quality_scale: "custom"`, but their `requirements` list is installed with the exact
   same code path, the same per-package retry budget (`MAX_INSTALL_FAILURES = 3`), and the same
   process-lifetime cache (`is_installed_cache`, `install_failure_history`) as core's own.
2. **Version specifiers are free-form for custom integrations; core enforces exact pinning on
   itself, not on you.** `script/hassfest/requirements.py`'s `validate_requirements_format` only
   raises `Requirement … need to be pinned "<pkg name>==<version>"` `if integration.core`. Custom
   integrations get format-only checks (valid PEP 508 syntax via `packaging.requirements.Requirement`,
   no embedded space) and are free to use `>=`, ranges, extras, or `pkg@git+https://…@<ref>` VCS
   syntax — this repo's own manifest already does so (`"beautifulsoup4>=4.0.0,<5.0.0"`).
   Additionally, **hassfest itself never runs against custom integrations at all** — it is a core
   dev-tooling script that only ever discovers integrations under `homeassistant/components/`; a
   HACS repository is not part of that tree and is never hassfest-validated in the first place, so
   even the format check is moot for us. HACS's own manifest validator (`INTEGRATION_MANIFEST_JSON_SCHEMA`
   in `hacs/integration`) doesn't look at `requirements` at all — `codeowners`, `documentation`,
   `domain`, `issue_tracker`, `name`, `version` are `vol.Required`; requirements is `vol.ALLOW_EXTRA`
   and untouched.
3. **`package_constraints.txt` is a pip/uv `--constraint` file, not a merge/negotiation step —
   core's pinned versions win any conflict silently, at install time, per package.** It's passed
   verbatim as `--constraint <path>` to `uv pip install` (`requirements.py:pip_kwargs`,
   `package.py:install_package`). A `--constraint` file doesn't request installation of anything;
   it only bounds what version `uv` may choose *if* it decides to install or upgrade that package.
   If a custom integration requests `some-pkg>=2.0` and `package_constraints.txt` pins
   `some-pkg==1.5.0` (because core itself depends on it), `uv` will refuse to satisfy both and the
   install fails; `install_package` returns `False`; `_install_with_retry` retries 3 times then
   gives up; the failing requirement lands in `RequirementsManager.install_failure_history`
   (persisted for the app lifetime) and `RequirementsNotFound` is raised, which surfaces as the
   integration failing to set up (config entry setup error / "Requirements for X not found").
   Nothing here silently succeeds with the wrong version and nothing crashes HA — the *specific*
   integration's setup fails while the rest of HA keeps running. There is no retry until "next
   configuration check or restart" per the log message quoted in the code.
4. **HA Core and HA Supervised are deprecated (support ended 2025.12); only HA OS and HA
   Container remain officially supported**, per the 2025-05-22 announcement. On both remaining
   methods, `/config` (and thus `/config/deps`) lives on the writable data partition — HAOS's
   read-only guarantee applies only to the system A/B partitions, not `/config` — so there is no
   read-only-filesystem obstacle to installing a custom integration's requirements. `pip_kwargs`
   targets `<config_dir>/deps` unless running in a venv or a docker/container environment
   (`is_docker_env()` checks `/.dockerenv`, `/run/.containerenv`, `KUBERNETES_SERVICE_HOST`, or
   `is_official_image()`); requirements are (re)installed on every integration load where the
   process-lifetime `is_installed_cache` doesn't already have them, i.e. effectively once per HA
   restart unless the cache already has that exact reqirement string, and the official
   `home-assistant.io` FAQ confirms deps are re-resolved/upgraded on every core version upgrade —
   they do **not** silently freeze across upgrades. All installation methods require outbound
   network access to PyPI (or whatever index is configured) at requirements-install time; there is
   no offline/air-gapped install story in core or in HACS — no vendored wheel cache, no bundled
   mirror.
5. **HA Core requires exactly Python `>=3.14.2`** — `REQUIRED_PYTHON_VER: Final[tuple[int, int, int]] = (3, 14, 2)`
   in `homeassistant/const.py`, matching `requires-python = ">=3.14.2"` in core's own
   `pyproject.toml`. A library whose floor is Python 3.14.2 is therefore never a constraint beyond
   what HA itself already imposes on every install — it cannot be *more* restrictive than HA's own
   floor by definition. Pure-Python code has no further concern; if the eventual shared library
   ever pulls in a compiled dependency, HAOS ships on `aarch64`/`amd64` official images (32-bit
   architectures deprecated alongside Core/Supervised, per the same announcement) — musllinux
   wheel availability is a per-dependency question, not something this research resolves, since our
   own `ha_external_statistics/` has zero compiled dependencies today (uses only `beautifulsoup4`,
   `yarl`, stdlib).
6. **HACS validates repository *shape* (manifest exists, has required keys, `hacs.json` matches
   its own schema) — it never touches `requirements`, never runs pip, and has no repo-to-repo
   dependency mechanism.** Confirmed by reading `hacs/integration`'s own validator
   (`validate/integration_manifest.py` → `INTEGRATION_MANIFEST_JSON_SCHEMA`) and its `hacs.json`
   schema (`HACS_MANIFEST_JSON_SCHEMA` in `utils/validate.py`, `vol.PREVENT_EXTRA` over
   `country`/`hacs`/`hide_default_branch`/`homeassistant`/`persistent_directory`/`render_readme`/
   `zip_release`/`name` — no field for declaring or requiring another HACS repository). **This
   confirms**: PyPI publication is the only first-class runtime mechanism two independently
   HACS-installed integrations have for sharing code, because HACS installs each repository's
   `custom_components/<domain>/` tree in isolation and the shared-code question is entirely
   delegated to whatever `requirements` says — HACS neither knows nor cares. The actual
   alternatives, with real costs:
   - **Vendoring a copy** (status quo): zero new install-time risk, but every bugfix must be
     hand-copied into both repos and can silently drift (already the case here: coolblue's
     `ha_external_statistics/` and greenchoice's are separate copies today).
   - **Git submodule**: HACS's clone-and-zip release pipeline does not resolve submodules (HACS
     downloads the `zip_release` artifact or a tarball of the tagged ref via GitHub API, not a
     recursive git clone) — a submodule pointing outside the repository would ship as an empty
     directory to installed users. Not viable through the standard HACS distribution path.
   - **Monorepo shipping two `custom_components/` folders from one HACS repo**: HACS supports
     `content_in_root: false` (already set in both this repo's and greenchoice's `hacs.json`) but a
     HACS repository maps to exactly one `custom_components/<domain>` category entry per HACS
     "repository" concept — HACS's repository model (`repositories/integration.py`) resolves a
     single integration domain per repo. Shipping two integrations from one repo is possible for
     manual/local installs but breaks HACS's per-repository indexing/discovery model and its
     one-domain-per-repository assumption; not a supported HACS pattern.
   - **A third, HACS-installable "library" repository**: HACS categories are
     `integration`/`plugin`/`theme`/`python_script`/`appdaemon`/`template` — there is no "library"
     category that gets installed into Python's import path rather than `custom_components/`.
     Not supported.
   - **PyPI package**: the only mechanism that is (a) supported by HACS's actual distribution
     model, (b) resolved automatically by core's requirements machinery at every restart, and
     (c) versioned/pinned independently of either consuming repo's release cadence.
7. **Real precedent exists for exactly this pattern — three verified examples.**
   - [`hacs/integration`](https://github.com/hacs/integration/blob/main/custom_components/hacs/manifest.json)
     requires `"aiogithubapi>=22.10.1"`;
     [`aiogithubapi`](https://pypi.org/project/aiogithubapi/) is maintained by the same author
     (`ludeeus`, HACS's own maintainer) — unpinned `>=`, consistent with custom integrations being
     free to choose.
   - [`cyberjunky/home-assistant-garmin_connect`](https://raw.githubusercontent.com/cyberjunky/home-assistant-garmin_connect/master/custom_components/garmin_connect/manifest.json)
     requires `"ha-garmin==0.1.31"`, exact-pinned; [`ha-garmin` on PyPI](https://pypi.org/project/ha-garmin/)
     is published by the same author (`author_email: Ron Klinkien <ron@cyberjunky.nl>`,
     `project_urls.Repository: https://github.com/cyberjunky/ha-garmin`) — verified `0.1.31` is
     the exact version on PyPI as of 2026-07-28, i.e. the manifest tracks PyPI head at time of
     writing.
   - [`alandtse/alexa_media_player`](https://raw.githubusercontent.com/alandtse/alexa_media_player/dev/custom_components/alexa_media/manifest.json)
     requires `"alexapy==1.29.25"`, exact-pinned, while
     [`alexapy` on PyPI](https://pypi.org/project/alexapy/) is currently at `1.30.0` — the manifest
     lags the newest release by one version, showing the breaking-change-handling pattern in
     practice: the integration's `codeowners` list both `@alandtse` (integration) and
     `@keatontaylor` (alexapy's actual PyPI author, `keatonstaylor@gmail.com`) as co-maintainers,
     and the manifest pin is bumped in a dedicated PR only after the new library version is
     verified compatible — new library releases do **not** propagate automatically; someone has to
     bump the manifest string and cut a new integration release.
   - **Vendoring precedent found in the same search**: [`sebr/bhyve-home-assistant`](https://github.com/sebr/bhyve-home-assistant)
     ships an empty `requirements` list and instead vendors its own API client as
     `custom_components/bhyve/pybhyve/` (confirmed via GitHub contents listing) — structurally
     identical to our current `ha_external_statistics/` vendoring, and evidence that vendoring
     remains a live, chosen pattern elsewhere in the HACS ecosystem, not a legacy mistake unique to
     us.
8. **Trusted Publishing (OIDC from GitHub Actions) is PyPI's current recommended release route,
   with no manually-managed long-lived token.** Per PyPI's own docs
   (`docs.pypi.org/trusted-publishers/`): a CI provider (GitHub Actions) acts as an OIDC identity
   provider; the project is configured on PyPI to trust a specific repo/workflow configuration;
   the CI job exchanges a short-lived OIDC token for a PyPI API token valid 15 minutes, used only
   to `twine upload`/`uv publish`. Minimum viable pipeline: a `pyproject.toml`-based build
   (`python -m build` or `uv build`), a GitHub Actions workflow triggered on tag push that builds
   the sdist/wheel and calls `pypa/gh-action-pypi-publish` with `id-token: write` permission and no
   stored secret. **On the `homeassistant.*` import question**: verified via
   `ldotlopez/ha-historical-sensor`'s own `pyproject.toml` — despite importing
   `homeassistant.components.sensor.SensorEntity` directly, its PyPI package
   (`homeassistant-historical-sensor`) declares **no** `homeassistant` dependency at all
   (`dependencies = ["importlib-metadata; python_version >= '3.14'"]`, PyPI's own
   `requires_dist` for the published `2.0.0` release is `null`). This is the load-bearing
   precedent: HA-coupled helper libraries rely on the fact that Home Assistant is already present
   in the runtime that imports them (the HA installation itself provides `homeassistant` on
   `sys.path`) rather than declaring it as an installable PyPI dependency — doing the latter would
   pull the entire multi-hundred-megabyte `homeassistant` package (and its own pinned dependency
   tree) into every install, and would couple the library's declared version range to a specific
   HA core release rather than to whatever HA version the user already has running.

---

## Body

### 1. Install path, hop by hop

`homeassistant/requirements.py` is the single chokepoint for both core and custom integrations:

```python
async def async_get_integration_with_requirements(
    hass: HomeAssistant, domain: str
) -> Integration:
    ...
    manager = _async_get_manager(hass)
    return await manager.async_get_integration_with_requirements(domain)
```

`RequirementsManager._async_process_integration` calls
`self.async_process_requirements(integration.domain, integration.requirements, integration.is_built_in)`
— `is_built_in` is passed through only to decide the wording of a deprecated-package warning log
line, nothing structural. `async_process_requirements` then:

```python
if not (missing := self._find_missing_requirements(requirements)):
    return
self._raise_for_failed_requirements(name, missing)

async with self.pip_lock:
    if missing := self._find_missing_requirements(requirements):
        await self._async_process_requirements(name, missing)
```

`_find_missing_requirements` filters against `self.is_installed_cache` (a per-`RequirementsManager`,
i.e. per-HA-process, set — never persisted to disk). `_raise_for_failed_requirements` re-raises
immediately, without retrying pip, for any requirement already recorded in
`install_failure_history`:

```python
for req in missing:
    if req in self.install_failure_history:
        _LOGGER.info(
            "Multiple attempts to install %s failed, install will be"
            " retried after next configuration check or restart",
            req,
        )
        raise RequirementsNotFound(integration, [req])
```

Actual installation (`_async_process_requirements` → `_install_requirements_if_missing` →
`_install_with_retry` → `pkg_util.install_package`) happens under a process-wide `asyncio.Lock`
(`self.pip_lock`), serializing all concurrent integration setups' installs, on the executor thread
pool (`hass.async_add_executor_job`) so it never blocks the event loop. `install_package` shells
out to `uv pip install <req> --quiet --index-strategy unsafe-first-match [--upgrade] [--constraint <path>] [--target <dir>]`.
Failures raise `RequirementsNotFound`, which propagates out of
`async_get_integration_with_requirements` and fails that integration's config-entry setup — this
is what the user sees: a "Failed to set up" / "Requirements … not found" error for that one
integration, not a global HA crash.

**Timing**: this all happens at *integration load* time — the first time something calls
`async_get_integration_with_requirements(hass, domain)` for that domain, which is triggered during
config-entry setup / component setup during HA startup (or when a config flow for that domain is
first initiated). It is not a separate "install phase" ahead of startup; a missing/failed
requirement delays that specific integration's readiness within the startup sequence.

**Caching**: `is_installed_cache` is checked before touching pip at all
(`pkg_util.is_installed(req)` inside `_install_requirements_if_missing`, which does a live
`importlib.metadata.version()` + specifier check) — so a requirement satisfied by an
already-installed package version is a no-op, no pip invocation. This is per-string-exact-match
against the *current requirements list*, not per-package: bumping the manifest's version string is
what triggers a fresh install attempt on the next restart.

### 2. Version specifier rules

`homeassistant/util/package.py:parse_requirement_safe` accepts anything `packaging.requirements.Requirement`
parses (full PEP 508: `==`, `>=`, `<=`, `~=`, `!=`, ranges via commas, extras `pkg[extra]`, environment
markers via `;`), plus a documented fallback for `pkg@git+https://...#fragment`-style strings for
backward compatibility with VCS installs. The developer docs
(`developers.home-assistant.io/docs/creating_integration_manifest/#requirements`) explicitly
document the git-VCS form:

> `"requirements": ["<library>@git+https://github.com/<user>/<project>.git@<git ref>"]`

and state under "Custom integration requirements":

> Custom integrations should only include requirements that are not required by the Core
> requirements.txt.

— advisory, not enforced by any code path found (nothing in `requirements.py`,
`util/package.py`, or hassfest checks for overlap with core's `requirements.txt`; a custom
integration *can* list a package core already ships, it just risks the constraint conflict
described in finding 3).

`script/hassfest/requirements.py`'s `validate_requirements_format` is the *only* format enforcement
in the whole pipeline, and it is core-only for the pin requirement:

```python
if not (match := PACKAGE_REGEX.match(req)):
    integration.add_error(
        "requirements", f'Requirement "{req}" does not match package regex pattern'
    )
    continue
pkg, sep, version = match.groups()

if integration.core and sep != "==":
    integration.add_error(
        "requirements",
        f'Requirement {req} need to be pinned "<pkg name>==<version>".',
    )
    continue
```

`integration.core` (`script/hassfest/model.py`) is `self.path.absolute().as_posix().startswith(self._config.core_integrations_path.as_posix())`
— a filesystem-path check against `homeassistant/components/`. hassfest is a `script/` module run
against core's own tree by core's own CI (`script/hassfest/main.py` scans that same
`core_integrations_path`); it is never invoked against a HACS repository's `custom_components/`
directory in any workflow discovered, so **this check never fires for us regardless of the pin
question** — negative result, not merely "doesn't apply because we're not core."

Net: for a custom integration, `==` is a free choice, not a requirement or even a convention
enforced anywhere in tooling. It is a convention practiced by careful maintainers (all three
precedent examples in finding 7 that pin, pin exactly), for the practical reason that unpinned
custom-integration requirements resolve against whatever the constraint file and already-installed
graph allow, at every restart, non-reproducibly.

### 3. Constraints and conflicts

`CONSTRAINT_FILE = "package_constraints.txt"`, resolved in `pip_kwargs`:

```python
def pip_kwargs(config_dir: str | None) -> dict[str, Any]:
    kwargs = {
        "constraints": os.path.join(os.path.dirname(__file__), CONSTRAINT_FILE),
        "timeout": PIP_TIMEOUT,
    }
    ...
```

and threaded into `install_package`:

```python
if constraints is not None:
    args += ["--constraint", constraints]
```

`homeassistant/package_constraints.txt` itself is header-stamped `# Automatically generated by
gen_requirements_all.py, do not edit` and lists exact `==` pins (with a few `>=` floors like
`certifi>=2021.5.30`) for every package core's own `requirements.txt`/`requirements_all.txt`
resolution produced — i.e. it mirrors core's own dependency graph, not a curated
compatibility policy layered on top of it.

A `--constraint` file in pip/uv semantics **only bounds versions of packages that get
installed/upgraded for some other reason** — it does not by itself request installation of
anything, and it does not silently substitute a different version into a package that already
satisfies the requirement string. If a custom integration's manifest requests `some-pkg>=2.0` and
`package_constraints.txt` pins `some-pkg==1.5.0`, `uv pip install "some-pkg>=2.0" --constraint
package_constraints.txt` fails outright (unsatisfiable constraint), `install_package` returns
`False`, `_install_with_retry` exhausts its 3 attempts, the requirement is recorded in
`install_failure_history`, and `RequirementsNotFound` propagates — the specific integration fails
to set up (visible in logs/UI as a failed config entry), while the rest of Home Assistant continues
running. There is no silent fallback to the constrained (older) version and no HA-wide crash; the
failure is scoped and logged (`_LOGGER.error("Unable to install package %s: %s", package, stderr)`
inside `install_package`), but nothing surfaces the *conflict itself* — the user sees a generic pip
failure string, not "this conflicts with package_constraints.txt".

### 4. Runtime environments

Core/Supervised deprecation (support ended with release 2025.12, per the 2025-05-22 announcement)
narrows the realistic target to **HA OS** and **HA Container**. On HA OS, the read-only guarantee
(`developers.home-assistant.io/docs/operating-system/partition/`) is scoped to the A/B system
partitions, swapped atomically on update; `/config` lives on the writable data partition (mounted
at `/mnt/data`) inside the Home Assistant Core container itself, so `<config_dir>/deps` is fully
writable at runtime on every supported installation method — there is no read-only-filesystem
obstacle to installing a custom integration's PyPI requirement on any currently supported HA
installation type.

`is_docker_env()` (`util/package.py`) detects `/.dockerenv`, `/run/.containerenv`,
`KUBERNETES_SERVICE_HOST`, or `is_official_image()`; when true, `pip_kwargs` does **not** set a
`target` directory, so `install_package` falls through to its user-site-packages branch inside the
container's own Python environment rather than `<config_dir>/deps` — i.e. on HAOS/Container the
package lands inside the (ephemeral, rebuilt-on-image-update) container filesystem's Python
environment, not on the persistent `/config` volume. The official `home-assistant.io` FAQ on
dependencies confirms the practical consequence either way: "after an upgrade the dependencies
will be upgraded as well" — nothing is frozen across a version bump; a new HA release re-resolves
and reinstalls requirements against the current `package_constraints.txt` on next start, which is
exactly the caching behavior traced in finding 1 (`is_installed_cache` is not persisted; only a
live `is_installed()` check protects against redundant installs).

There is no offline/air-gapped installation story anywhere in this pipeline — `uv pip install`
needs to reach PyPI (or whatever index is configured) every time a requirement isn't already
satisfied, and neither core nor HACS ships a vendored wheel cache or bundled mirror.

### 5. Python version and wheels

`REQUIRED_PYTHON_VER: Final[tuple[int, int, int]] = (3, 14, 2)` (`homeassistant/const.py`) and
`requires-python = ">=3.14.2"` (`pyproject.toml`) match exactly, confirming HA's own floor is
3.14.2 — identical to this repo's stated floor. A library that also floors at 3.14.2 therefore adds
no additional constraint versus what any HA install already enforces. Since our own
`ha_external_statistics/` has zero compiled dependencies (only `beautifulsoup4`, `yarl`, and
stdlib), the musl/aarch64 wheel-availability question that matters for compiled extensions on HAOS
does not currently apply to this specific extraction; it would need separate verification only if
the shared library grew a native dependency.

### 6–8: see Answer above — the body would only restate points already fully quoted and cited
there without adding new evidence.

---

## Confidence and limits

- Findings 1–3 (install path, pinning rule, constraint-file mechanics) are **verified** directly
  against pinned Home Assistant core source at a confirmed commit SHA, quoted verbatim. High
  confidence.
- The claim that **hassfest never runs against custom/HACS integrations** is verified by the
  `Integration.core` path-prefix check in `script/hassfest/model.py` plus the absence of any
  hassfest invocation against `custom_components/` in the repos read; it is not verified against
  every possible third-party CI configuration a HACS author might construct, but is true of the
  standard, documented hassfest/HACS pipelines. `[INFERENCE]` only insofar as "some HACS author
  could theoretically vendor hassfest into their own CI" is a hypothetical this note doesn't rule
  out.
- HACS's own source was read at `main` (unpinned to a release tag) since HACS ships no separate
  version-tagged "spec" document; the validator/schema files read are stable across the recent
  history of that repo based on their simplicity and lack of open PRs touching them at fetch time,
  but this is `[INFERENCE]` rather than a pinned-commit guarantee like the core files.
- The **runtime-environment** section (§4) synthesizes an official 2025 deprecation announcement
  with an official OS-partitioning developer doc; the specific claim "no read-only obstacle to
  `/config/deps` on any currently supported install method" is a direct **inference** from those
  two primary sources (partition boundary + is-docker-env branching in core code), not a single
  citation that states it outright — flagged `[INFERENCE]`.
- Precedent examples in finding 7 are **verified**: each manifest was fetched directly from the
  named GitHub repo, and each PyPI project's author/maintainer metadata was fetched directly from
  the PyPI JSON API, on 2026-07-28. The claim that `alexapy`'s manifest pin "lags by one version
  deliberately, bumped only after compatibility is verified" is `[INFERENCE]` from the observed pin
  (`1.29.25`) trailing PyPI's current release (`1.30.0`) plus the shared-codeowners fact — the
  actual PR history bumping that pin was not read to confirm the causal claim about *why* it lags.
- Wheel/musl availability (end of §5) is explicitly scoped as **not resolved here** — flagged as
  out of scope for the current dependency set, not claimed to be fine in general.
- No PyPI package currently importing `homeassistant.components.recorder` specifically (as opposed
  to `homeassistant.components.sensor`, which is what `ha-historical-sensor` does) was found and
  verified; the `homeassistant` non-dependency pattern is established via the closest available
  precedent (a helper library importing a different `homeassistant.components.*` module), and
  extended to the recorder case by analogy — flagged `[INFERENCE]`.

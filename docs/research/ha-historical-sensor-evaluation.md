# Should coolblue_energy adopt an existing library instead of publishing `ha_external_statistics/`?

**Question.** `docs/research/backfilling-external-statistics.md` §Answer point 5 dismissed
`ldotlopez/ha-historical-sensor` in five lines while investigating an unrelated seed-sum bug. Is
that dismissal correct? Should `coolblue_energy` adopt `ha-historical-sensor` — or any other
existing library — instead of extracting its own `ha_external_statistics/` package (already
duplicated once, into `homeassistant-greenchoice`) into a published PyPI library?

**Why we asked.** This is the gating decision for a larger effort to plan and execute that
extraction. If an existing library is structurally usable and materially better, extracting our
own is largely moot; if not, the extraction plan proceeds on solid ground rather than an
unexamined five-line dismissal.

---

## Method and provenance

Primary sources only: pinned commit permalinks, PyPI JSON, GitHub API JSON, and LICENSE files.
No blog posts or secondary write-ups used as evidence.

| Source | URL | Fetched | Pinned identity |
|---|---|---|---|
| `ha-historical-sensor` source | `https://github.com/ldotlopez/ha-historical-sensor` | 2026-07-28 | `git clone --depth 50`, checked out [`96e1d70a9e381761a3fdae1f4f4d458df81a2b16`](https://github.com/ldotlopez/ha-historical-sensor/commit/96e1d70a9e381761a3fdae1f4f4d458df81a2b16) (HEAD of `main`, committed 2026-04-11T18:30:46+02:00, *"refactor: consolidate polling logic and bump version"*) — same commit the prior note pinned, re-verified via `git log -1` on the freshly cloned tree |
| `ha-historical-sensor` repo metadata | `https://api.github.com/repos/ldotlopez/ha-historical-sensor` | 2026-07-28 | JSON: `stargazers_count: 59`, `open_issues_count: 9`, `license.spdx_id: "GPL-3.0"`, `pushed_at: 2026-04-11T16:44:54Z` |
| `ha-historical-sensor` releases | `https://api.github.com/repos/ldotlopez/ha-historical-sensor/releases` | 2026-07-28 | newest tag `v3.0.0a5`, published 2026-04-11T16:45:09Z |
| `ha-historical-sensor` issues | `https://api.github.com/repos/ldotlopez/ha-historical-sensor/issues?state=all` and `search/issues?q=repo:ldotlopez/ha-historical-sensor+is:issue+statistic` | 2026-07-28 | issues #7, #10, #18, #19, #20 read in full |
| `ha-historical-sensor` PyPI metadata | `https://pypi.org/pypi/homeassistant-historical-sensor/json` | 2026-07-28 | `info.version: "2.0.0"` (latest non-prerelease); release history from `0.0.1` (2023-01-10) through `3.0.0a5` |
| `ha-historical-sensor` git commit log | `git log --since="24 months ago"` on the clone | 2026-07-28 | 14 commits in the trailing 24 months; latest before the pin dated 2026-01-05, then a burst 2026-04-07 to 2026-04-11 |
| Dependents search | `gh api search/code -f q='"homeassistant-historical-sensor" filename:manifest.json'` and `filename:pyproject.toml` | 2026-07-28 | 8 manifest.json hits across 8 distinct repos (one is the library's own `delorian` test integration); 1 pyproject.toml hit (the library's own) |
| `klausj1/homeassistant-statistics` | `https://api.github.com/repos/klausj1/homeassistant-statistics` | 2026-07-28 | `license.spdx_id: "MIT"`, `stargazers_count: 176`, `open_issues_count: 10`, `pushed_at: 2026-07-27T08:43:22Z`; pinned at [`5f46aa5790f702ac555f9fa505412a1d5b15123d`](https://github.com/klausj1/homeassistant-statistics/commit/5f46aa5790f702ac555f9fa505412a1d5b15123d) by the prior backfill note |
| Core `dev` head negative result | reused from `docs/research/backfilling-external-statistics.md`, same-day (2026-07-28) exhaustive grep of the full `dev` tree at [`07a2383bdc4f1a71b0b695e24cbc0c1da823f99b`](https://github.com/home-assistant/core/commit/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b) | 2026-07-28 | not re-cloned independently — see caveat below |
| This repo's `LICENSE` | `LICENSE:1-3` | 2026-07-28 | `MIT License / Copyright (c) 2026 Baris Demirdelen` |
| `ha-historical-sensor`'s `LICENSE` | `/tmp/librarian-ha-historical-sensor/LICENSE:1-4` | 2026-07-28 | `GNU GENERAL PUBLIC LICENSE / Version 3, 29 June 2007` |
| `homeassistant-greenchoice`'s `LICENSE` | `/home/burger/Projects/homeassistant-greenchoice/LICENSE:1-3` | 2026-07-28 | `MIT License / Copyright (c) 2019 Jesse van Leth` |
| Our vendored package | `custom_components/coolblue_energy/ha_external_statistics/{external_statistic,recorder,statistics_mixin}.py` and `tests/ha_external_statistics/` | 2026-07-28 | current working tree |
| Sibling consumer | `/home/burger/Projects/homeassistant-greenchoice/custom_components/greenchoice/{hourly_statistics.py,coordinator.py}` | 2026-07-28 | current working tree |

Greps used: `has_sum\|mean_type\|unit_class\|def build_stat_data\|def inject\(\|running_sum\|StatisticMeanType` over `external_statistic.py`; `def async_get_last_sum\|def async_inject_day\|lookback_hours\|query_start` over `recorder.py`; `def __init__\|def async_run_statistics_update\|def _async_process_day_range\|def _async_retry_recent_days\|def _async_backfill\|def async_reimport_statistics\|except UpdateFailed\|abstractmethod\|def _process_day` over `statistics_mixin.py`; `class HistoricalSensor\|def get_statistic_metadata\|def async_calculate_statistic_data\|def _async_write_statistics\|async_add_external_statistics\|has_sum=False\|UPDATE_INTERVAL` over the library's `sensor.py`.

**Caveat on provenance.** Point 7's re-verification of "core ships no statistics-importer helper"
reuses the exhaustive `dev`-tree grep performed the same day (2026-07-28) for
`docs/research/backfilling-external-statistics.md`, pinned at `07a2383bdc…`, rather than
re-cloning and re-grepping independently. Re-running the same exhaustive search against the same
commit within the same day would not change the result; this is stated explicitly as reuse, not
independent re-derivation. `[INFERENCE]`: treating same-day, same-pin reuse as sufficient
re-verification rather than mandating a second clone.

---

## Answer

1. **The library is structurally entity-bound, and that is load-bearing, not incidental.**
   `HistoricalSensor(SensorEntity)` ([`sensor.py:39`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/sensor.py#L39))
   derives its statistic ID from the entity: `statistic_id=self.entity_id.replace(".", ":", 1)`
   is the documented pattern (README, "Define the `statistic_id` property… you can use the
   entity_id") and the default `get_statistic_metadata` sources it from
   `self.entity_id.split(".", 1)[0]`
   ([`sensor.py:188`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/sensor.py#L188)),
   and the write path reads `self.entity_id` directly in every log line
   ([`sensor.py:175-176`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/sensor.py#L175-L176)).
   There is no code path that builds a `StatisticMetaData` or calls the write method without an
   entity instance to hang `self.entity_id`, `self.hass`, and `self.name` off of. Using it means
   creating an entity per statistic, i.e. reversing ADR 0001.

2. **It does call `async_add_external_statistics` (colon-prefixed, entity-free API), but only as
   an implementation detail behind an entity-shaped facade.**
   `_async_write_statistics` calls `async_add_external_statistics(self.hass, statistics_metadata,
   statistics_data)`
   ([`sensor.py:173`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/sensor.py#L173)) —
   the same colon-prefixed, entity-independent recorder call our own `external_statistic.py:175`
   uses. This is a *recent* change: GitHub issue
   [#18](https://github.com/ldotlopez/ha-historical-sensor/issues/18) ("(3.x) Major Breaking
   Change: State Writing Removed, Statistics-Only Model", opened by the maintainer
   2025-12-29T19:16:02Z) documents that versions before `3.x` wrote raw `States`/`StatesMeta` rows
   directly — "~300 lines of complex code" doing state-chain reconstruction — and that this was
   dropped as "extremely complex and fragile" in favour of the official statistics API alone. So
   the library's *history* is a cautionary tale for hand-rolled recorder writes (a risk our own
   package never took on, since it always used the public `async_add_external_statistics`/
   `statistics_during_period` API — see `docs/research/backfilling-external-statistics.md` §1).
   But the *current* write path calling the entity-free API is still gated behind
   `SensorEntity.async_added_to_hass()` / `async_track_time_interval` scheduling
   ([`sensor.py:84-119`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/sensor.py#L84-L119)):
   the statistic is only ever written as a side effect of an entity's lifecycle, never on its own.
   There is no `async_import_statistics` call anywhere in the package (grep for
   `async_import_statistics` over `homeassistant_historical_sensor/` returns no matches — only the
   entity-free `async_add_external_statistics`), so at least the *statistic-writing call itself*
   would not force `entity_id`-derived (dot-prefixed, converted) statistic IDs on us the way
   `async_import_statistics` would. That distinction is real but narrow: the facade in front of it
   still requires an entity.

3. **`HistoricalSensor`'s own statistics machinery is close to a no-op you must build yourself,
   entity or not.** `get_statistic_metadata` hardcodes `has_sum=False` and
   `mean_type=StatisticMeanType.NONE`
   ([`sensor.py:185-186`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/sensor.py#L185-L186))
   — a caller wanting `sum` statistics (which the Energy Dashboard requires for consumption) must
   override the method entirely, not extend it. `async_calculate_statistic_data` is `raise
   NotImplementedError()`
   ([`sensor.py:206-208`... see `:196-208`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/sensor.py#L196-L208)):
   there is no running-sum accumulation, no seed logic, no gap handling anywhere in the library —
   every one of those is left entirely to the subclass, same as if there were no library at all.
   The one thing the base class supplies for free is the overlap cutoff: `_async_write_statistics`
   drops any `hist_states` at or before `latest_statistic_data["start"] + 3600`
   ([`sensor.py:163-167`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/sensor.py#L163-L167)),
   fetched via `hass_get_last_statistic`, itself a thin wrapper over `get_last_statistics(hass, 1,
   …)`
   ([`helpers.py:53-70`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e381761a3fdae1f4f4d458df81a2b16/homeassistant_historical_sensor/helpers.py#L53-L70)).

---

## Capability matrix, ours vs theirs

| Capability | `ha_external_statistics/` (ours) | `ha-historical-sensor` (`96e1d70a`) |
|---|---|---|
| Running-sum accumulation across hours | **Provided.** `ExternalStatistic.build_stat_data` accumulates `running_sum += value` per entry (`external_statistic.py:119-129`). | **Not provided.** `async_calculate_statistic_data` is abstract (`sensor.py:196-208`); caller writes their own accumulation loop. |
| Seeding the sum from the recorder | **Provided.** `async_get_last_sum` queries a 25 h window via `statistics_during_period` (`recorder.py:23-70`). | **Partially provided** as a *building block*, not a seed: `hass_get_last_statistic` fetches the single latest row (`helpers.py:53-70`), but only to compute the overlap *cutoff timestamp* (`sensor.py:163-167`); nothing extracts or forwards its `sum` to the caller's calculation. |
| Gap / empty-day handling | **Provided.** `_process_day` returning `None` resets the seed to force a fresh DB query next day; a failed day preserves the last seed instead of resetting to zero (`statistics_mixin.py:108-170`, comment at `:128-137` of the same). | **Not provided.** No day/period concept exists in the library at all — it processes whatever `historical_states` the caller populated, on a fixed 30 s-default poll timer (`sensor.py:40`), with no notion of a calendar day succeeding or failing. |
| DST-safe local-day→UTC hour mapping | **Provided by the caller, using the package's `datetime`-in/`datetime`-out contract.** `period_start_fn` takes a raw entry plus a reference `date` and returns an aware UTC `datetime` (`external_statistic.py:58-59,84`); our `statistics.py` (not evaluated here) and the sibling's `hourly_statistics.py:22-24` implement the actual local→UTC conversion via `dt_util.as_utc`. | **Left entirely to the caller.** `HistoricalState.timestamp` is a bare `float` (`helpers.py:22-26`); no timezone or DST handling exists anywhere in the package. |
| Idempotent overwrite of already-written periods | **Provided for free by the recorder API itself** (`docs/research/backfilling-external-statistics.md` §Answer point 2, §1.2): `async_add_external_statistics` overwrites on exact `start_ts` match. Both packages rely on this recorder guarantee identically — neither adds anything on top. | **Actively works against it.** The library *filters out* any state at or before `latest["start"] + 3600` before ever building `StatisticData` (`sensor.py:163-167`), so a reimport of an already-covered period is a client-side no-op, not a server-side overwrite — the opposite of our reimport action, which depends on overwrite semantics to rewrite explicit historical ranges (`statistics_mixin.py:191-212`). |
| Backfill window on first setup | **Provided.** `_stats_backfilled` gates a `BACKFILL_DAYS`-long first pass (`statistics_mixin.py:37-49,68-99,180-189`). | **Not provided.** No first-run/backfill concept; `async_update_historical` is called once at `async_added_to_hass` and then on the poll timer (`sensor.py:106-119`) — whatever the caller returns is whatever gets written, with no distinction between "first ever run" and "routine poll". |
| Trailing retry window on every poll | **Provided.** `_async_retry_recent_days` re-processes `RETRY_DAYS` on every non-first call (`statistics_mixin.py:174-178`). | **Not provided.** Same gap as above — the caller's `async_update_historical` implementation would have to invent this. |
| User-triggered reimport of an explicit date range | **Provided.** `async_reimport_statistics(start_date)` (`statistics_mixin.py:191-212`), wired to a Home Assistant service action in `__init__.py`. | **Not provided.** No date-range API exists; the closest analogue, CSV import, is a two-and-a-half-year-old open feature request ([issue #7](https://github.com/ldotlopez/ha-historical-sensor/issues/7), opened 2023-03-29, still open; referencing [issue #3](https://github.com/ldotlopez/ha-historical-sensor/issues/3)). |
| `mean_type`/`unit_class` metadata (mandatory HA 2026.11) | **Provided.** `ExternalStatistic.metadata` always sets both (`external_statistic.py:95-103`). | **Partially provided, and was broken by the requirement.** The base class hardcodes `mean_type=StatisticMeanType.NONE` and a caller-supplied `unit_class`; getting this wrong throws at entity-add time. [Issue #19](https://github.com/ldotlopez/ha-historical-sensor/issues/19) (opened 2026-04-11, closed 2026-04-17) is exactly this failure mode in production — a real user (`shtrom`, via a downstream integration `AuroraPlusHA`) hit `HomeAssistantError: Unsupported unit_class: 'monetary'` from `_async_import_statistics` because the library required `unit_class` to be set but the correct value for a monetary sensor is `None`, and the library did not know that. [PR #17](https://github.com/ldotlopez/ha-historical-sensor/pull/17) ("add `mean_type` and `unit_class` to metadata", opened 2025-11-12) was the fix; it was still open (unmerged) as of the `96e1d70a` pin and merged only 2026-04-30 — three weeks *after* the pinned commit. |
| Typed generic over the caller's raw API entry type | **Provided.** `ExternalStatistic[T]` (`external_statistic.py:38`) — `period_start_fn`/`value_fn`/`mean_fn`/`min_fn`/`max_fn` are all `Callable[[T], …]`. | **Not provided.** `HistoricalState` (`helpers.py:21-26`) is a fixed, non-generic dataclass (`state: Any`, `timestamp: float`, `attributes: dict`); the caller's raw API type never appears in the library's type surface. |

Nine capabilities we enumerate from our own source; the library provides one in full (metadata
carrying, and even that had a real production defect fixed only after the pin), one partially as a
side effect of a different feature (overlap cutoff, which actively works against our idempotent-
reimport requirement), and leaves the other seven entirely to the caller.

---

## The cost of adopting

Adopting `ha-historical-sensor` for `coolblue_energy` would require, concretely:

1. **Creating six entities**, one per statistic, each a `HistoricalSensor` subclass — directly
   reversing `docs/adr/0001-statistics-only-no-entities.md`. The ADR's stated reason for having
   *no* sensors was that "each one only ever restated yesterday's daily total... duplicating" the
   statistics; a `HistoricalSensor` reports `STATE_UNKNOWN` permanently (per the library's own
   FAQ: "Historical sensors don't provide the current state... The current state is unknown"), so
   the entity created to satisfy the library's architecture would show `unknown` in the UI forever
   — worse than the six sensors the ADR already rejected, not better.
2. **The Energy Dashboard would see the same colon-prefixed statistic**, since
   `async_add_external_statistics` is the same call either way (finding 2). What it would
   additionally see is six always-`unknown` entities cluttering the entity list and device page the
   ADR deliberately keeps empty.
3. **`quality_scale.yaml` would have to un-exempt `entity-unique-id`, `has-entity-name`, and
   `entity-event-setup`** (`custom_components/coolblue_energy/quality_scale.yaml`, currently all
   three `exempt` with comment "This integration does not create its own entities.") and instead
   satisfy them for six new, permanently-`unknown` entities — paperwork with no functional payoff.
   `appropriate-polling` (currently `done`, "Polls every 6 hours") would also need rework: the
   library's own poll loop defaults to `UPDATE_INTERVAL = timedelta(seconds=30)`
   (`sensor.py:40`) unless overridden per entity, a completely different polling model from our
   single 6-hour `DataUpdateCoordinator` tick.
4. **We would still have to reimplement, on top of the library, every capability row marked "not
   provided" above** — sum accumulation, seeding, gap handling, DST mapping, backfill window,
   retry window, reimport action — i.e. we would still need something functionally equivalent to
   our entire `ha_external_statistics/` package, just wrapped inside six entity shells whose only
   contribution is `STATE_UNKNOWN` and an overlap-cutoff filter that fights our reimport action
   (capability row "idempotent overwrite"). Net: strictly more code and more moving parts than what
   we run today, for a UI surface the ADR already rejected on the record.
5. **A GPL-3.0 runtime dependency** (§Licence below) — a separate cost, not a functional one.

---

## The case FOR adopting

Steelmanned, not dismissed:

- **It genuinely absorbs one real, if narrow, piece of maintenance: the `mean_type`/`unit_class`
  metadata churn from the October 2025 recorder-statistics-API change** (cited in
  `docs/research/backfilling-external-statistics.md` §Method, "Changes to the recorder statistics
  API", 2025-10-16). The library hit this breakage in production ([issue #19](https://github.com/ldotlopez/ha-historical-sensor/issues/19))
  and fixed it upstream ([PR #17](https://github.com/ldotlopez/ha-historical-sensor/pull/17)) —
  proof the maintainer does track recorder API changes, even if slowly (three weeks after our pin).
  Depending on a library in principle means someone else absorbs the *next* such change instead of
  us. In practice, our own package already carries this metadata correctly today
  (`external_statistic.py:92-103`), so this benefit is prospective, not something we are currently
  missing.
- **State-class-derived, entity-backed statistics genuinely come for free — for a *different*
  shape of integration.** If `coolblue_energy` ever needed a live, pollable "current state" sensor
  *in addition to* the historical statistics (which the ADR explicitly declined to keep), an
  entity-based library is the correct tool, and HA computes statistics automatically for any
  `state_class` sensor without any external-statistics machinery at all. That is not this
  integration's shape, but it is the shape the library is actually built for.
- **UI visibility as a debugging aid.** The ADR itself flags, as *not rejected on merit*, "a
  smaller diagnostic surface (for example a last-successful-import timestamp)" as a legitimately
  separate idea. If that idea is ever pursued, a `HistoricalSensor`-style entity (or, more simply,
  a plain diagnostic sensor with no statistics involvement at all) is a reasonable building block —
  but it is orthogonal to the six consumption/cost statistics this evaluation is about.
- **Someone else's continuous-integration and pre-commit tooling.** The repo has GitHub Actions
  (CodeQL, a PyPI release workflow) and `.pre-commit-config.yaml`
  — infrastructure we would not have to write ourselves if we depended on it as a package rather
  than vendoring our own.
- **Would an entity-backed design have solved the seed-sum bug?**
  (`docs/research/backfilling-external-statistics.md` §Answer point 1). **No — the library's own
  cutoff-based overlap handling is a *different* bug shape, not a fix for ours.** Our bug is a
  stale 25-hour lookback window that returns `0.0` when the most recent row is more than 25 hours
  old (`recorder.py:28,51-70`), corrupting the running `sum`. The library sidesteps the seeding
  problem entirely by not seeding at all — it filters overlapping timestamps
  (`sensor.py:163-167`) and leaves sum *computation* fully to the caller's
  `async_calculate_statistic_data`. A caller using the library would still have to write their own
  seed-fetch logic (there being no `has_sum=True` support built in at all — capability row above),
  and could reproduce exactly the same 25-hour-window bug, or a worse one, since the library offers
  zero guidance on how to seed a sum correctly. Adopting the library would not have prevented this
  bug; it would have relocated the exact same responsibility into integration-specific code with
  even less scaffolding than we have today.
- **Is there a hybrid?** A worthwhile middle path exists, but it is not "depend on
  `ha-historical-sensor`": it is what this repo and `homeassistant-greenchoice` have *already*
  done — vendor a small, dependency-free, generic module (`ha_external_statistics/`) that both
  repos independently copied verbatim and that neither needed to modify to fit a second, materially
  different consumer shape (EUR cost statistics with per-item `consumed_on` timestamps in
  `greenchoice/hourly_statistics.py`, vs. day-batched kWh/m³ sums with API-label-derived timestamps
  here). That is the working hybrid: shared code, no entity requirement, no GPL, already field-
  tested by two independent integrations.

---

## Licence

- **This repo:** MIT (`LICENSE:1-3`, "MIT License / Copyright (c) 2026 Baris Demirdelen").
- **`homeassistant-greenchoice` (the sibling that vendors the same package):** MIT
  (`/home/burger/Projects/homeassistant-greenchoice/LICENSE:1-3`, "MIT License / Copyright (c)
  2019 Jesse van Leth").
- **`ldotlopez/ha-historical-sensor`:** GPL-3.0 — the repository's `LICENSE` file is the verbatim
  GNU GPL v3 text (`/tmp/librarian-ha-historical-sensor/LICENSE:1-4`), every source file carries a
  GPL-2-or-later header ("you can redistribute it and/or modify it under the terms of the GNU
  General Public License as published by the Free Software Foundation; either version 2... or (at
  your option) any later version" — `sensor.py:1-16`, `__init__.py:1-16`), and GitHub's own
  license detection agrees: `license.spdx_id: "GPL-3.0"`
  (`https://api.github.com/repos/ldotlopez/ha-historical-sensor`).

`[INFERENCE]` What a GPL-3.0 runtime dependency implies for this repo: GPL-3.0 is a strong
copyleft licence whose distribution obligations attach to combined/derivative works. A HACS custom
integration that *imports* a GPL-3.0 package at runtime (rather than merely being *used alongside*
it as an independent, separately-installed HA add-on) is the kind of "combined work" GPL-3.0 §5
addresses, and a plausible reading requires the combined work to also be distributed under
GPL-3.0-compatible terms — which MIT is (MIT can be relicensed into a GPL combination), but the
practical effect is that this repository's *effective* distribution licence for anyone consuming it
together with the dependency would functionally become GPL-3.0, not MIT, for the combined
artifact. Whether HACS's install-as-a-git-checkout distribution model (rather than a bundled wheel)
changes this analysis is a real legal question this evaluation does not resolve; it is flagged
here, not adjudicated. This is a judgement call, not a verified legal conclusion — treat it as a
reason to avoid the dependency rather than as legal advice.

---

## Maintenance and adoption reality

- **Release history (PyPI, `homeassistant-historical-sensor`):** first published `0.0.1` on
  2023-01-10; the run of point releases (`0.0.1` → `0.0.5`) spans January–June 2023; the current
  PyPI-reported stable (`info.version`) is **`2.0.0`**; the newest tag on GitHub is
  **`v3.0.0a5`, published 2026-04-11T16:45:09Z** — an alpha (`a5`) pre-release by PEP 440
  semantics, so `pip install homeassistant-historical-sensor` without `--pre` still resolves to
  `2.0.0`, not the current `3.x` statistics-only rewrite this evaluation is actually about.
  Anyone depending on the published package via ordinary version pinning is on the *old*,
  state-writing-into-raw-`States`-rows codebase that issue #18 calls "extremely complex and
  fragile" — worse, not better, than adopting nothing.
- **Commit cadence:** `git log --since="24 months ago"` on the clone returns **14 commits**. Nine
  of those fall in a single five-day burst, 2026-04-07 to 2026-04-11 (the `3.x` rewrite and its
  release), preceded by two commits on 2026-01-05 ("Fix pypi workflow", "Minor fixes") and nothing
  else visible in the trailing two years before that burst. This is bursty, single-maintainer
  activity, not a steady support treadmill.
- **Issues:** `open_issues_count: 9` (GitHub API, includes PRs by GitHub's counting convention).
  Read in full: #7 (CSV import example, open since 2023-03-29, unresolved for over three years),
  #10 (usage question about forcing a `state`, open since 2023-08-21, one unhelpful reply), #18
  (the maintainer's own breaking-change announcement, open, self-described as "final and
  non-negotiable"), #20 (a Spanish-language user asking how to replace lost cost-helper
  functionality after the 3.x rewrite, open, zero replies as of fetch time). #19 (the
  `unit_class`/statistics-metadata production bug discussed above) is closed, with a fix that took
  from PR submission (2025-11-12) to merge (2026-04-30) — over five months.
- **Python / HA version support:** `pyproject.toml` declares `requires-python = ">=3.14"`
  and a dev-only dependency `homeassistant>=2026.2.3`; there is no runtime `homeassistant` pin in
  `dependencies` (it is assumed to be provided by the host environment, standard for HA custom
  components) — so the package **does not pin `homeassistant`**, it floats against whatever HA
  version imports it.
- **Verified dependents.** `gh api search/code` for `"homeassistant-historical-sensor"` in
  `manifest.json` returns 8 hits across 8 repositories; one is the library's own `delorian` test
  integration, one (`n00bcodr/homeassistant`) is a personal HA config mirroring another
  integration's files rather than a distinct project. That leaves **six verified independent
  third-party integrations**: `ldotlopez/ha-ideenergy` (the maintainer's own energy-provider
  integration — the README's first "current projects" example), `LeighCurran/AuroraPlusHA`,
  `barreeeiroo/Home-Assistant-Electric-Ireland`, `PBrunot/energy_owl_hacs`,
  `CorentinGrard/Veolia`, `jiri-muller/cez_pnd`. A `filename:pyproject.toml` search for the same
  string returns only the library's own `pyproject.toml` — no downstream project depends on it via
  `pyproject.toml`; every real dependent is a HACS-style `manifest.json`
  requirement, consistent with how HACS custom integrations declare Python dependencies.

---

## Field sweep for alternatives

| Candidate | What it is | Verdict |
|---|---|---|
| `ldotlopez/ha-historical-sensor` | Entity-based historical-data importer, now statistics-only internally (v3.x) but still requires a `SensorEntity` per statistic. GPL-3.0. Stable PyPI release (`2.0.0`) predates the statistics-only rewrite; the rewrite is only available as an unpublished-to-stable pre-release (`3.0.0a5`). | **Dismissed** — see findings 1-3 and the capability matrix above. |
| `klausj1/homeassistant-statistics` | A full HACS custom integration (not a library) providing HA *service actions* to import/export long-term statistics from CSV/TSV/JSON files, no entities, no dashboard cards (per its own GitHub description). MIT-licensed. 176 stars, `open_issues_count: 10`, actively pushed (`pushed_at: 2026-07-27`, i.e. the day before this evaluation). | **Not a base to build on, but a real complementary tool.** It is a manually-triggered, file-driven one-shot importer for humans fixing history gaps by hand — not a coordinator-driven, continuously-polling library a custom integration's own update loop can call. Its purpose (ad hoc CSV backfill) does not overlap with `coolblue_energy`'s (continuous, API-driven, day-granular statistics). Worth knowing about as a *user-facing* tool, not as a dependency. |
| `ha-statistics` (as a package name) | No PyPI package or notable GitHub repository found under this exact name. | **Does not exist as a distinct candidate** — searched and dismissed. |
| `external-statistics` (as a package name) | No PyPI package found; matches only documentation/blog mentions of the recorder API concept, not a library. | **Does not exist as a distinct candidate.** |
| `ha-recorder` / "HA External Recorder" (`ha_recorder_ext`) | A different custom integration entirely: a *complementary recorder* for ML/analytics feature engineering (per its Home Assistant Community forum post), not a statistics-writing helper. Solves the opposite direction of data flow (reading HA history out for analysis) from what `coolblue_energy` needs (writing external history in). | **Out of scope, dismissed on purpose mismatch.** |
| `historical` (generic search term) | Returns `ha-historical-sensor` itself and unrelated tools (`bokub/ha-history-stats`, a history-graph statistics analyzer for existing entities, not an importer). | No additional candidate found. |
| Home Assistant core, `homeassistant/helpers/` | Checked for a shared statistics-importer helper. | **Re-confirmed negative, same-day.** `docs/research/backfilling-external-statistics.md` §Method already ran the exhaustive grep `async_add_external_statistics\|async_import_statistics\|external_statistics` over `homeassistant/helpers/**` at `dev` head [`07a2383bdc4f1a71b0b695e24cbc0c1da823f99b`](https://github.com/home-assistant/core/commit/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b) (2026-07-28, tree identity verified by blob-SHA comparison against the GitHub API) with **zero matches**. This evaluation reuses that same-day result rather than re-cloning the ~500 MB tree a second time within the same day; see the Method table caveat. |

No candidate outside `ha-historical-sensor` and `klausj1/homeassistant-statistics` surfaced in
either the PyPI-oriented web search or the targeted GitHub code searches.

---

## Verdict

**Keep vendoring `ha_external_statistics/`; do not adopt `ha-historical-sensor` or any other
existing library.** The prior five-line dismissal in `docs/research/backfilling-external-statistics.md`
is correct, and the pros/cons examination here does not change the outcome — it sharpens *why*:

1. **The library's core design premise — an entity per statistic — directly reverses ADR 0001**,
   and the entity it would force into existence reports `STATE_UNKNOWN` forever by the library's
   own design, which is a worse UI outcome than the six sensors the ADR already rejected on the
   record, not a better one.
2. **Even ignoring the entity requirement, the library supplies almost none of the actual hard
   part.** Of the nine capabilities in our vendored package, the library provides one in full
   (statistics metadata carrying — and that one had a real production bug, live for over five
   months, fixed only three weeks after the commit the prior note evaluated), one partially and in
   a way that actively conflicts with our reimport action, and leaves the remaining seven —
   including the entire running-sum/seed/gap/backfill/retry state machine that *is* the hard part
   of this problem — to the caller. Adopting it would not have prevented the seed-sum bug
   documented separately; it would have relocated the same responsibility with less scaffolding.
3. **The GPL-3.0 licence is a genuine, independent objection** on top of the architectural one, for
   an MIT-licensed HACS integration already shared verbatim, licence-compatibly, with a second MIT
   repository (`homeassistant-greenchoice`).
4. **The "hybrid" the steelman surfaces already exists**: vendoring a small, dependency-free,
   generic module and copying it between repos is precisely what this repo and
   `homeassistant-greenchoice` have already done, field-tested against two materially different
   consumer shapes without modification. That is the correct shape for this problem; publishing it
   as a proper (still MIT, still entity-free) PyPI package is a packaging change to *our own*
   working design, not an adoption of someone else's.

**Named condition that would flip this verdict:** if `coolblue_energy` ever needs a
continuously-polled, currently-valued sensor entity *in addition to* the historical statistics —
i.e. if the ADR's own noted-but-not-pursued idea of a diagnostic or live-value entity is revived —
then `HistoricalSensor` (or a plain `SensorEntity`) becomes a legitimate, on-purpose tool for
*that* narrow need, evaluated on its own merits and licence cost at that time. It would still not
replace `ha_external_statistics/` for the six statistics-only writes this integration exists to
produce.

---

## Confidence and limits

- High confidence on findings 1-3 (library structure), the capability matrix, the licence
  identifiers, and the PyPI/GitHub release and issue data — all read directly from pinned source,
  JSON API responses, or LICENSE files with line/commit citations.
- Medium confidence on "verified dependents" (point 6): GitHub code search is documented elsewhere
  in this repo's research notes as partial/best-effort, not exhaustive; six confirmed real
  dependents is a **lower bound**, not an exact count.
- The GPL-3.0 combined-work implication (§Licence) is explicitly marked `[INFERENCE]` — a
  plausible legal reading, not adjudicated legal advice. It is offered as one clear, independent
  reason to prefer MIT tooling, not as the sole basis for the verdict.
- The core-`dev`-head re-verification (field sweep, last row) reuses a same-day exhaustive result
  from a sibling research note rather than independently re-cloning; this is disclosed as a
  provenance caveat, not hidden.
- This note evaluates whether to **adopt** an existing library; it does not itself decide *how* to
  package `ha_external_statistics/` for publication (packaging mechanics, versioning, CI) — that
  remains the separate, now-unblocked extraction effort.

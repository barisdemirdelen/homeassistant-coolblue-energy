# The public surface of `ha_external_statistics`

**Status.** Decided 2026-07-28 by
[What is the library's public surface?](https://github.com/barisdemirdelen/homeassistant-coolblue-energy/issues/20).
This document holds the interface that ticket chose. It is *not* the handoff spec — distribution,
testing, the HA support matrix, the seeding contract's fine detail and both migration paths are
other tickets, assembled by
[Assemble the handoff spec](https://github.com/barisdemirdelen/homeassistant-coolblue-energy/issues/26).

**How it was chosen.** Four interfaces were designed independently and in parallel, each under a
different constraint — minimise the surface, maximise flexibility, optimise for the modal caller,
and ports-and-adapters — then compared on depth, locality and seam placement. The minimal-surface
design won, with one graft. §4 records what the other three contributed and what was rejected.

---

## 1. The surface

Five public symbols. Two of them a caller may never spell.

```python
__all__ = [
    "DayReadings",
    "ImportReport",
    "NothingImported",
    "Statistic",
    "StatisticsImporter",
]
```

```python
@dataclass(frozen=True, slots=True)
class Statistic:
    """One external statistic: what to call it and what it measures.

    ``statistic_id`` must contain exactly one ``':'`` with a non-empty side each —
    the prefix is the owning domain, and the colon is what marks the series as
    externally owned rather than entity-backed. Raises ``ValueError`` at
    construction otherwise, so a malformed id fails when your module imports and
    not on day three of a backfill.
    """

    statistic_id: str
    unit: str
    name: str | None = None
```

```python
type DayReadings = Mapping[Statistic, Sequence[tuple[datetime, float]]]
"""One local calendar day's readings, grouped by the statistic they belong to.

Each pair is a reading: the hour it starts, and the value measured in it. The
datetime may be naive (read in the importer's timezone) or aware (used as given).
A statistic absent from the mapping simply has no readings that day — that is a
partially published day, not an error.
"""
```

```python
@dataclass(frozen=True, slots=True)
class ImportReport:
    """What one run did. Returned on success and on partial failure alike."""

    days: tuple[date, ...]  # attempted, ascending
    imported: tuple[date, ...]  # wrote at least one row
    pending: tuple[date, ...]  # the utility has published nothing yet
    failed: tuple[date, ...]  # fetch raised
    rows_written: int
    seed_reads: int  # recorder queries issued
    error: Exception | None  # the last fetch failure, if any

    @property
    def imported_anything(self) -> bool: ...

    def summary(self) -> str:
        """One log line: '5 imported, 1 pending, 1 failed (2026-03-26: HTTP 502)'."""
```

```python
class NothingImported(Exception):
    """A run wrote no rows at all.

    ``__cause__`` is the last exception raised by ``fetch``, or ``None`` when every
    day was simply unpublished — the difference between "the service is down" and
    "the utility has not caught up yet".
    """
```

```python
class StatisticsImporter:
    def __init__(
        self,
        hass: HomeAssistant,
        fetch: Callable[[date], Awaitable[DayReadings]],
        *,
        timezone: tzinfo | None = None,
        backfill_days: int = 7,
        retry_days: int = 3,
    ) -> None: ...

    async def async_import(self) -> ImportReport:
        """Run one import cycle.

        The first cycle that imports anything backfills ``backfill_days`` calendar
        days ending yesterday; every cycle after that re-imports the last
        ``retry_days``. Raises ``NothingImported`` if the cycle wrote no rows — in
        which case the backfill is *not* marked done and the next call re-attempts
        the whole window rather than narrowing to the retry days.
        """

    async def async_reimport(self, since: date) -> ImportReport:
        """Recompute and overwrite ``since`` through yesterday, inclusive.

        Never affects whether the backfill is considered done. Raises ``ValueError``
        if ``since`` is later than yesterday, ``NothingImported`` if the range
        produced no rows.
        """
```

Two required constructor arguments, three defaulted keywords, two coroutines.

### Why each parameter survived

| Parameter | Kept because |
|---|---|
| `hass` | Irreducible — the recorder is reached through it. |
| `fetch` | Irreducible — it *is* the seam. |
| `timezone` | Genuinely differs: coolblue's API publishes in a fixed `Europe/Amsterdam` whatever HA is set to; greenchoice uses HA's own zone. Defaults to `dt_util.DEFAULT_TIME_ZONE`, so greenchoice omits it. |
| `backfill_days`, `retry_days` | Neither consumer passes them — both want the defaults. They exist so a third consumer with a slower publication lag need not fork. Zero cost to a caller who never learns they exist. |

### What was deleted from the vendored package, and why the caller can no longer get it wrong

| Deleted | Replaced by |
|---|---|
| `source` | `statistic_id.split(":", 1)[0]`. Core *enforces* `metadata["source"] == split_statistic_id(statistic_id)[0]` and raises `HomeAssistantError("Invalid source")` otherwise. The field could only ever be right or fatal. |
| `unit_class` | Looked up from `unit` via `STATISTIC_UNIT_TO_UNIT_CONVERTER`, else `None` — core's own fallback. Because the class is looked up *from* the unit, `unit_of_measurement in converter.VALID_UNITS` holds by construction and `HomeAssistantError("Unsupported unit_class")` becomes unreachable. That is the exact production failure `ha-historical-sensor` shipped on a monetary statistic; both consumers here write EUR statistics. |
| `has_sum` | Always `True`. See §3. |
| `mean_type`, `mean_fn`, `min_fn`, `max_fn` | Always `StatisticMeanType.NONE`. See §3. |
| `period_start_fn`, `value_fn`, the `[T]` generic | The caller builds `(datetime, float)` pairs inside `fetch`, where its raw entry type already is. |
| `StatisticsLoopMixin`, `_process_day`, `seed_sums` | Gone. Seeds are never named in the interface. |
| `async_get_last_sum`, `async_inject_day`, `lookback_hours` | Gone. The 25-hour window is deleted, not fixed. |

---

## 2. The interface beyond the signatures

**Ordering.** Days are processed strictly ascending, oldest first; the caller cannot influence the
order. `fetch` is awaited **at most once per day, sequentially, entirely within the awaited
`async_import()` / `async_reimport()` call** — never concurrently, never after the call returns.

That one sentence is load-bearing: it is what lets greenchoice hold a session-scoped client open
across an entire run with no library support at all. No session parameter, no lifecycle hook, no
context-manager port — the caller's own `async with` already spans the run.

**Invariants.**

- Every written row carries `state` (the hour's own value) and `sum` (cumulative), so
  `sum[n] − sum[n−1] == state[n]` holds by construction for every series the library writes.
- Readings are sorted by resolved UTC before use; caller ordering is irrelevant.
- Two readings resolving to the same UTC hour are **merged by addition**. Correct only because
  values are additive deltas — a consequence of §3, and the fix for the DST fall-back hour, where
  today the second row silently overwrites the first.
- A reading resolving outside `[day_start, next_day_start)` is rejected with `ValueError`, naming
  the statistic. This is the loud failure for a wrong `timezone`.
- The backfill is marked done by a **total function of the run's outcome**, not by an assignment
  positioned below a `raise`: `done = done or report.imported_anything`.
- Runs are serialised by an internal lock, so a reimport arriving mid-poll cannot interleave two
  seed chains over the same days.
- Idempotent: the recorder upserts on exact `start_ts`, so re-running any range with identical
  fetch results produces identical rows. The library adds **no** dedupe logic.

**Error modes.**

| Condition | Result |
|---|---|
| `fetch` raises `ConfigEntryAuthFailed` | Propagates unchanged, immediately. No later day attempted. Backfill stays un-done. |
| `fetch` raises anything else | Day recorded in `failed`; the run continues. |
| `fetch` returns `{}`, or every sequence empty | Day recorded in `pending`. Ordinary, not an error. |
| Run wrote zero rows | `NothingImported`, chained from the last fetch exception if there was one. |
| A reading falls outside the day's local bounds | `ValueError`, immediately — a caller bug, deterministic. |
| Two `Statistic`s in one mapping share an id | `ValueError`. |
| `async_reimport(since)` with `since > yesterday` | `ValueError`. Today's mixin logs a warning and silently does nothing; a user-triggered action must not no-op. |

**Performance.** Per run of *N* days: one batched `statistics_during_period` read on the first day
that produces data, one more after any hole, plus at most one `get_last_statistics` per
never-before-written statistic on a cold start. Today's code issues six separate reads per range.
Writes are one `async_add_external_statistics` enqueue per statistic per day — ~18/poll and
~42/backfill for coolblue, noise against the recorder queue's 65 000-entry ceiling, buying per-day
durability and self-healing of holes inside the retry window. Memory is one day of readings
regardless of range length, so a 365-day reimport is flat.

**Required configuration.** None beyond the constructor. The consumer's `manifest.json` should
carry `"after_dependencies": ["recorder"]`.

### Pull, not push

Under **push** the caller drives the loop and hands days in. It then owns day-range arithmetic,
ascending order, when to seed versus chain, what an empty day means for the next day's seed, what a
failed day means, and the "imported nothing" escalation — five of the nine behaviours leave the
library. Symbol count might stay low while the *interface* explodes, because interface is
everything a caller must know, not everything they must type. That is a large interface wearing a
small hat.

Under **pull**, the caller's entire obligation is: *given a local calendar day, return that day's
readings.* The one thing push buys — a session held open across the loop — is bought instead by the
ordering guarantee above, for free.

### The HA compatibility contract — 11 symbols

```python
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    STATISTIC_UNIT_TO_UNIT_CONVERTER,
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.core import HomeAssistant  # annotation only
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.util.dt import DEFAULT_TIME_ZONE
```

Deliberately absent: `homeassistant.helpers.update_coordinator` (no `DataUpdateCoordinator`, no
`UpdateFailed`) and `homeassistant.config_entries`. Those are the surfaces that churn hardest, and
the decision to track current HA stable only makes every imported symbol a standing cost.

`homeassistant` is **not** a declared dependency: `dependencies = []`, `requires-python = ">=3.14"`,
MIT.

---

## 3. Scope: cumulative sums only

`has_sum` is always `True` and `mean_type` always `NONE`. Mean, min and max statistics are out of
scope, and a caller needing one should call `async_add_external_statistics` directly — three lines
this library would add nothing to.

Three reasons, in order of weight:

1. Seeding, chaining and gap handling — the library's entire value — have no meaning without a sum.
2. The additive same-hour merging in §2 would be **wrong** for a mean.
3. Neither consumer uses them, and none of core's eleven statistics writers in this shape does
   either.

This is a scoped narrowing, not a regression against the nine-capability floor: the capability
recorded there is *carrying `mean_type`/`unit_class` correctly*, which this design does, and does
more safely than the current code by making the failure mode unreachable.

The typed generic `ExternalStatistic[T]` is also dropped. It looked like type safety but was a
round trip: `T` was threaded from the caller's list, through the library, into the caller's own
lambdas, and never touched anything the library computed. Its only real work was making
`(ExternalStatistic[MeterReadingEntry], list[MeterReadingEntry])` type-check as a pair at the
`async_inject_day` call site — and that call site no longer exists, because the caller never hands
the library a raw entry. The caller's entries stay fully typed inside its own `fetch`.

---

## 4. What the rejected designs contributed

**Grafted in.** `ImportReport` — the one addition beyond the minimal four symbols. Without it a
partially-failed run is indistinguishable from a clean one, and both consumers log skipped days
today. The information already exists inside the library, which logs a warning and throws it away;
returning it costs one frozen dataclass and makes the library observable rather than
read-the-logs. Exceptions stay reserved for "nothing happened at all". Also grafted: validating the
statistic id at construction rather than at write time, and expressing the backfill flag as a total
function of the outcome instead of an assignment whose position below a `raise` is what makes it
correct.

**Rejected, with reasons.**

- **A `StatisticsStore` port** (ports-and-adapters design). It has exactly one production adapter
  and always will. Its in-memory twin must reimplement upsert-by-period — a second implementation
  of recorder semantics that can drift from the real one. Decisive counter: gap-proof seeding is
  *precisely* the behaviour a hand-written fake gets wrong, so testing it against a fake tests the
  fake. The 25-hour bug survived its whole life against tests; a real recorder would have caught
  it, a port would not have. `pytest-homeassistant-custom-component` makes the recorder locally
  substitutable, which is what removes the testability argument for the port.
  Worth keeping from that design regardless: *write the correctness rule down as a contract*, not
  as an implementation detail — here, as the documented contract of the private seed read plus its
  regression test.
- **`Plan`-as-data** (maximum-flexibility design): `Chunk`/`Plan` with `trailing()`/`chunks()`/
  `months()` replacing the private window methods. Genuinely the best idea for extensibility, and
  the right answer if a provider with no per-day endpoint, sub-hourly resolution, or monthly
  correction sweeps ever needs serving. Deferred, not dismissed: it costs two concepts in the
  common path for consumers that do not exist yet, and it can be added later without breaking the
  five symbols above.
- **Owning the poll timer and config-entry setup** (modal-caller design). It does kill the
  no-op-listener trap this repo already documents, but it buys `ConfigEntry`,
  `async_track_time_interval` and `ConfigEntryNotReady` into the compatibility contract. Against a
  decision to track current HA stable only, that is the wrong trade: the most turnkey design also
  had the largest HA surface.
- **`auth_errors` / `transient_errors` tuples.** Two ways to say one thing. Three of the four
  designs converged on the zero-config answer instead: raise from your own `fetch`, where your
  API's exception hierarchy is already in scope.

---

## 5. What this settles for the consumers

- **Statistic definitions stay caller-owned**, permanently. Coolblue declares six module-level
  constants; greenchoice builds its set per config entry with slugified ids. No shared shape exists
  and the library never tries to own one.
- **The local-day→UTC mapping moves into the library**, parameterised by one `timezone` value.
  Both consumers' `_day_start_utc` disappears, as does coolblue's `_entry_to_utc` and `_ts` and
  greenchoice's `_as_utc_start` and its manual `sorted(...)`.
- **Reimport is library-provided as a method**; registering the service action, its schema and its
  config-entry lookup stay per-integration. Greenchoice additionally wraps it in its own session.
- **Coolblue loses its coordinator entirely.** It creates no entities, so its `DataUpdateCoordinator`
  was a timer in a costume, propped up by a no-op listener and returning a `CoordinatorData` nothing
  consumes. It becomes an importer plus a three-line `async_track_time_interval`.
- **Greenchoice keeps its coordinator**, because it has real sensor entities fed by a different
  call. The importer is invoked from inside its existing `async with self.api:` block.

### A latent bug this surfaces

`StatisticsLoopMixin._today()` returns `dt_util.now().date()` — *Home Assistant's* today. Coolblue's
days are Amsterdam days. Whenever HA is configured to any other zone, the day the loop asks for and
the day the API publishes disagree near midnight. Enumerating days in the importer's own timezone
fixes it; it ships with the extraction, alongside the seeding fix.

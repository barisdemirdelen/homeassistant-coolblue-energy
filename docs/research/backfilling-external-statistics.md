# Backfilling external statistics into the Home Assistant recorder

**Question.** Is the way `coolblue_energy` backfills long-term statistics optimal? Are there Home
Assistant core conventions for it, or existing libraries that do it for us?

**Why we asked.** The whole product of this integration is six external statistics written into the
recorder (`docs/adr/0001-statistics-only-no-entities.md`), so the backfill loop *is* the
integration. It currently walks one calendar day at a time and calls
`async_add_external_statistics` once per statistic per day
(`custom_components/coolblue_energy/ha_external_statistics/external_statistic.py:175`), seeding each
day's running `sum` from a 25-hour-window recorder query
(`custom_components/coolblue_energy/ha_external_statistics/recorder.py:51-62`). Nobody had checked
that shape against what core actually does, or against what the recorder actually guarantees.

---

## Method and provenance

GitHub's code-search index is partial, so the entire `dev` tree was downloaded and grepped
exhaustively instead.

- Source: `https://codeload.github.com/home-assistant/core/tar.gz/refs/heads/dev`, fetched
  **2026-07-28** (19:17 UTC).
- Tree identity **verified** against commit
  [`07a2383bdc4f1a71b0b695e24cbc0c1da823f99b`](https://github.com/home-assistant/core/commit/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b)
  (`dev` head, committed 2026-07-28T18:47:51Z, *"Adapt zwave_js to new device registry API
  (#176959)"*). Method: `git hash-object <path>` on the extracted tree compared against
  `GET /repos/home-assistant/core/contents/<path>?ref=07a2383…` → `.sha`. Six blobs matched, all
  load-bearing for this note:

  | Path | Blob SHA (local == remote) |
  |---|---|
  | `homeassistant/components/recorder/statistics.py` | `641dc5e2973a2db37ad71d112e9badd5889148de` |
  | `homeassistant/components/recorder/__init__.py` | `0968f1fd54906c25840628f3e669652bf729187a` |
  | `homeassistant/components/recorder/tasks.py` | `d383161a553f644db1fd5730484bf6ac082cdc82` |
  | `homeassistant/components/recorder/websocket_api.py` | `f77a31b93d008798ff0961d86bd23fb161b65da5` |
  | `homeassistant/components/opower/coordinator.py` | `670edce22dbbcc69122dc8a391722c9495823ca6` |
  | `homeassistant/components/ista_ecotrend/sensor.py` | `978bdfedf77bff2274f433d9026f41e5c3e3f26b` |

- `homeassistant/const.py` reports version **`2026.8.0.dev0`** (`MAJOR_VERSION = 2026`,
  `MINOR_VERSION = 8`, `PATCH_VERSION = "0.dev0"`, lines 24-26).
- Note for cross-referencing: `docs/research/statistics-only-integrations.md` pinned
  `504fddd216b0dee295a58217037a32b7e8d2a166` earlier the same day. `dev` advanced during 2026-07-28;
  both commits are `2026.8.0.dev0`. Every permalink **in this note** is pinned to `07a2383…`, and
  every line number below is from that commit.

Greps used (all over the extracted tree, all case-sensitive regex):

| Purpose | Pattern | Scope | Result |
|---|---|---|---|
| Find every statistics writer | `async_add_external_statistics\|async_import_statistics` | `homeassistant/components` | 11 integrations + `recorder` itself |
| Locate the API surface | `def async_add_external_statistics\|def async_import_statistics\|def _insert_statistics\|def _update_statistics\|def get_last_statistics\|def get_last_short_term_statistics\|def statistic_during_period\|def statistics_during_period\|def adjust_statistics` | `recorder/statistics.py` | 12 hits |
| Find the queue path | `def async_import_statistics\|ImportStatisticsTask\|def async_adjust_statistics\|AdjustStatisticsTask` | `recorder/` | `core.py`, `tasks.py`, `statistics.py` |
| Queue mechanics | `commit_before\|def queue_task\|_process_one_task_or_event_or_recover\|MAX_QUEUE_BACKLOG\|backlog` | `recorder/core.py` | 20 hits |
| **Look for an importer helper** | `async_add_external_statistics\|async_import_statistics\|external_statistics` | `homeassistant/helpers` | **no matches** |
| **Look for any cumulative-sum helper** | `def .*(cumulative\|running_sum\|build_statistic\|backfill\|import_history\|seed_sum)` | `homeassistant/` | only per-integration privates (`suez_water._build_statistics`, `waterfurnace._build_statistics`/`_async_backfill`, `huisbaasje._get_cumulative_value`, and two unrelated `overkiz` `_control_backfill`) |
| Per-integration survey | `get_last_statistics\|get_last_short_term_statistics\|statistics_during_period\|statistic_during_period\|async_add_external_statistics\|async_import_statistics\|update_interval\|async_track_time_interval\|async_track_time_change\|SCAN_INTERVAL` | each writer's package | see table below |
| Sum-repair callers | `_adjust_sum_statistics\(\|def _augment_result_with_change` | `recorder/statistics.py` | 4 hits, all inside `adjust_statistics` |

Developer docs: `https://developers.home-assistant.io/sitemap.xml` fetched 2026-07-28 (630 `<loc>`
entries), filtered with `statistic|recorder|energy|database|db-schema` — six hits, all blog posts,
**zero documentation pages**. Candidate pages `docs/core/entity/sensor` and
`docs/integration_fetching_data` were fetched with `Accept: text/markdown` and read.

Libraries: `ldotlopez/ha-historical-sensor` source read at pinned commit
[`96e1d70a9e38`](https://github.com/ldotlopez/ha-historical-sensor/commit/96e1d70a9e38) (HEAD of
`main`, 2026-04-11T16:30:46Z); `klausj1/homeassistant-statistics` at
[`5f46aa5790f702ac555f9fa505412a1d5b15123d`](https://github.com/klausj1/homeassistant-statistics/commit/5f46aa5790f702ac555f9fa505412a1d5b15123d)
(2026-07-07). PyPI metadata from `https://pypi.org/pypi/homeassistant-historical-sensor/json`.

---

## Answer

**No, it is not optimal — but the gap is one specific bug plus one cheap query, not the
architecture.** The per-day loop is a legitimate variant of what core does (core's own highest-graded
writers rewrite a trailing window on every refresh, exactly as our retry-days pass does), and our own
`ha_external_statistics/` package is a better foundation than anything on PyPI. The defect is the
seed read.

1. **The seed read is wrong in a way that permanently corrupts the `sum` series.**
   `async_get_last_sum` looks back a fixed 25 hours (`recorder.py:28,51`). If the newest existing row
   is older than that — HA offline for two days, an outage longer than `RETRY_DAYS`, a cleared
   statistic, or simply a mid-range day the provider never published: `_process_day` returns `None`
   for an empty day (`coordinator.py:130-134`), which resets the seed to `None`
   (`statistics_mixin.py:142,145`) and forces the *next* day to query a 25-hour window that covers
   only the day with no rows — the query returns nothing and the function returns `0.0`
   (`recorder.py:70`). The day is then written with sums restarting near zero. The recorder never
   recomputes sums
   ([`statistics.py#L2915-L2919`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2915-L2919)),
   and the Energy Dashboard renders `change = sum − previous sum`
   ([`statistics.py#L2087`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2087),
   consumed at
   [`energy/websocket_api.py#L282`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/energy/websocket_api.py#L282)),
   so the result is one enormous negative bar followed by a permanent offset. The 25 hours were
   chosen to "cover DST" (`recorder.py:34-35,47-49`) but DST cannot move the previous row: statistics rows
   are UTC-hourly, so the row before a local-midnight boundary is *always* exactly one hour earlier
   (verified below). The window buys nothing and costs correctness.

2. **Everything else about the write path is safe.** `async_add_external_statistics` is a `@callback`
   that validates and enqueues — *"This inserts an import_statistics job in the recorder's queue"*
   ([`statistics.py#L2863-L2873`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2863-L2873))
   — and re-inserting an existing period **updates in place**, never duplicates
   ([`statistics.py#L2915-L2919`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2915-L2919)).
   Our reimport service and our overlapping retry window are therefore idempotent by construction,
   with no extra work needed.

3. **There is no core convention to adopt beyond the read-back idiom, because there is no shared
   code.** All eleven core writers hand-roll the same four steps, and the *only* thing they agree on
   is the seeding idiom: `get_last_statistics(1, …)` as an "have I ever written this?" probe, then a
   point-in-time `statistics_during_period` read for the actual baseline sum. `solaredge` is the
   closest analogue to us (many statistic IDs, no sensors, a trailing overwrite window) and shows the
   correct, gap-proof shape in 40 lines
   ([`solaredge/coordinator.py#L540-L582`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L540-L582)).

4. **Negative result: core exposes no helper for importers, and the developer docs say nothing about
   external statistics.** Nothing under `homeassistant/helpers/` mentions the statistics API; nothing
   in `homeassistant/components/recorder/` builds cumulative sums for a caller; the only public entry
   points are the two `async_*_statistics` functions, the `recorder/import_statistics` websocket
   command
   ([`websocket_api.py#L566-L623`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/websocket_api.py#L566-L623)),
   and `recorder/adjust_sum_statistics`. `recorder/services.yaml` exposes `purge`, `purge_entities`,
   `disable`, `enable`, `get_statistics` — **no import action**. The finding from
   `docs/research/statistics-only-integrations.md` still holds as of 2026-07-28: **no
   developers.home-assistant.io page documents external statistics at all**, and the newest
   statistics-related developer-blog post is still
   [2025-10-16 "Changes to the recorder statistics API"](https://developers.home-assistant.io/blog/2025/10/16/recorder-statistics-api-changes),
   which is a `mean_type`/`unit_class` deprecation notice, not guidance.

5. **Negative result: no library is a better foundation than `ha_external_statistics/`.** The only
   real candidate, `ldotlopez/ha-historical-sensor`, subclasses `SensorEntity`
   ([`sensor.py:39`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L39))
   — it *requires* the entities our ADR deliberately removed — leaves sum computation abstract
   (`async_calculate_statistic_data` raises `NotImplementedError`), has no backfill-window, gap, or
   correction logic, is GPL-3.0, and its newest release is a pre-release (`3.0.0a5`, 2026-04-11).
   Keeping ours is the right call.

6. **Our per-day call granularity is a defensible divergence, not a defect.** Core writers make one
   call per statistic per refresh carrying the whole fetched range; we make one per statistic per
   day, i.e. 6 × 7 = 42 enqueues during a backfill and 6 × 3 = 18 per poll. Each enqueued
   `RecorderTask` triggers a commit first (`commit_before = True`,
   [`tasks.py#L28-L37`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/tasks.py#L28-L37),
   applied at
   [`core.py#L896-L898`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/core.py#L896-L898)),
   but the queue's stop-recording ceiling is 65 000 entries
   ([`recorder/const.py#L28`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/const.py#L28)),
   so ~72 tasks/day is noise. In exchange we get per-day durability and self-healing of holes inside
   the retry window, which the strictly-after core writers (`elvia`, `tibber`, `ista_ecotrend`)
   cannot do at all.

---

## What we do today

Precisely, so the comparison below is fair:

- **Driver.** `CoolblueCoordinator` is a `DataUpdateCoordinator` with
  `update_interval = SCAN_INTERVAL = timedelta(hours=6)` (`const.py:8`); `_async_update_data` does
  nothing but call `async_run_statistics_update()` (`coordinator.py:112-114`).
- **Three entry points.** First call backfills `BACKFILL_DAYS = 7` days
  (`const.py:9`, `statistics_mixin.py:180-189`); every later call re-processes the last
  `RETRY_DAYS = 3` days (`const.py:13`, `statistics_mixin.py:174-178`); the
  `coolblue_energy.reimport_statistics` action re-processes `start_date … yesterday`
  (`statistics_mixin.py:191-212`, wired at `__init__.py:72-78`).
- **One calendar day at a time, oldest → newest.** `_async_process_day_range` iterates the day list
  in ascending order (`statistics_mixin.py:108-170`), calling `_process_day`
  (`coordinator.py:123-146`) per day. A day that raises is skipped and reported once per range; the
  last successful seed is deliberately *preserved* rather than reset, which treats the failed day as
  zero consumption instead of producing a negative spike (`statistics_mixin.py:128-137`).
- **Per-statistic running `sum`, seeded from the recorder.** `async_inject_day` reuses the previous
  day's end-of-day sums when chaining, and otherwise queries the DB per statistic
  (`recorder.py:116-126`) via `async_get_last_sum`, which runs
  `statistics_during_period(day_start − 25 h, day_start, {stat_id}, "hour", None, {"sum"})` on the
  recorder executor and takes the last row (`recorder.py:51-70`).
- **One `async_add_external_statistics` call per statistic per day.** `build_stat_data` accumulates
  `running_sum += value` per hour and stamps each point with `sum`
  (`external_statistic.py:119-140`, accumulation at `:127-129`); `inject` fires the recorder call if
  the day produced any points (`external_statistic.py:175`). Six statistics per day
  (`coordinator.py:220-233`), all with correct post-2026.11 metadata (`mean_type`, `unit_class`;
  `external_statistic.py:92-103`, `statistics.py:77-141`).
- **Hour starts are computed from Amsterdam-local labels.** `_entry_to_utc` converts the API's
  `"HH:MM"` label plus the calendar date to UTC (`statistics.py:48-62`); `_day_start_utc` gives the
  UTC instant of local midnight (`statistics.py:65-67`).

---

## 1. What the recorder statistics API guarantees

### 1.1 It is a queue enqueue, not a write

`async_add_external_statistics` is a `@callback`. It validates, then delegates to
`_async_import_statistics`, whose own docstring is *"Validate timestamps and insert an
import_statistics job in the queue"*
([`statistics.py#L2765-L2771`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2765-L2771)):

```python
@callback
def async_add_external_statistics(
    hass: HomeAssistant,
    metadata: StatisticMetaData,
    statistics: Iterable[StatisticData],
    *,
    _called_from_ws_api: bool = False,
) -> None:
    """Add hourly statistics from an external source.

    This inserts an import_statistics job in the recorder's queue.
    """
```
([`statistics.py#L2862-L2873`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2862-L2873))

The chain, hop by hop:

| Hop | Code | Citation |
|---|---|---|
| validate + enqueue | `get_instance(hass).async_import_statistics(metadata, statistics, Statistics)` | [`statistics.py#L2823-L2824`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2823-L2824) |
| queue the task | `self.queue_task(ImportStatisticsTask(metadata, stats, table))` | [`core.py#L608-L616`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/core.py#L608-L616) |
| recorder thread runs it | `ImportStatisticsTask.run` → `statistics.import_statistics(...)`, re-queueing itself if it did not finish | [`tasks.py#L197-L215`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/tasks.py#L197-L215) |
| DB work, retryable | `@retryable_database_job("statistics")` + `session_scope(exception_filter=filter_unique_constraint_integrity_error(...))` | [`statistics.py#L2994-L3011`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2994-L3011) |

Two consequences that matter for cost:

- **Nothing about the call is synchronous or blocking.** Our loop does not need
  `async_add_executor_job` around it, and does not (`external_statistic.py:175`). Correct.
- **Every task forces a commit before it runs**, because `RecorderTask.commit_before = True` by
  default and `ImportStatisticsTask` does not override it
  ([`tasks.py#L28-L37`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/tasks.py#L28-L37),
  [`tasks.py#L197-L203`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/tasks.py#L197-L203)),
  and the recorder thread honours it:

  ```python
  if task.commit_before:
      self._commit_event_session_or_retry()
  task.run(self)
  ```
  ([`core.py#L896-L898`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/core.py#L896-L898))

  So *N* calls cost *N* commits plus *N* task dequeues, versus one commit for one call carrying the
  same rows. The queue itself only misbehaves at `MAX_QUEUE_BACKLOG_MIN_VALUE = 65000` combined with
  low free memory
  ([`recorder/const.py#L28-L29`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/const.py#L28-L29),
  [`core.py#L378-L386`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/core.py#L378-L386)),
  which our 42-call backfill is five orders of magnitude away from.

Note also that external statistics are written **only to the long-term `Statistics` table** — the
table argument is hard-coded
([`statistics.py#L2824`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2824)).
That single fact settles the read-back question in §1.4 and means our rows are never purged: `purge`
only ever deletes *short-term* statistics rows
([`purge.py#L96-L103`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/purge.py#L96-L103),
[`purge.py#L504-L511`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/purge.py#L504-L511)).

### 1.2 Re-inserting an existing period overwrites; it never duplicates

This is the load-bearing guarantee for our reimport action and our overlapping retry window:

```python
    now_timestamp = time_time()
    for stat in statistics:
        if stat_id := _statistics_exists(session, table, metadata_id, stat["start"]):
            _update_statistics(session, table, stat_id, stat)
        else:
            _insert_statistics(session, table, metadata_id, stat, now_timestamp)
```
([`statistics.py#L2914-L2919`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2914-L2919))

The existence probe is an exact `start_ts` equality on `(metadata_id, start_ts)`:

```python
def _statistics_exists(
    session: Session,
    table: type[StatisticsBase],
    metadata_id: int,
    start: datetime,
) -> int | None:
    """Return id if a statistics entry already exists."""
    start_ts = start.timestamp()
    result = (
        session.query(table.id)
        .filter((table.metadata_id == metadata_id) & (table.start_ts == start_ts))
        .first()
    )
    return result.id if result else None
```
([`statistics.py#L2749-L2762`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2749-L2762))

and the update writes **every** value column, so a stale `mean`/`min`/`max` cannot survive a rewrite:

```python
        session.query(table).filter_by(id=stat_id).update(
            {
                table.mean: statistic.get("mean"),
                table.min: statistic.get("min"),
                table.max: statistic.get("max"),
                table.last_reset_ts: datetime_to_timestamp_or_none(
                    statistic.get("last_reset")
                ),
                table.state: statistic.get("state"),
                table.sum: statistic.get("sum"),
            },
            synchronize_session=False,
        )
```
([`statistics.py#L901-L921`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L901-L921))

Metadata is upserted the same way, per call, before the rows
(`statistics_meta_manager.update_or_add`,
[`statistics.py#L2907-L2913`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2907-L2913)) —
so re-declaring identical metadata 42 times a backfill is harmless, just redundant.

**Duplicate rows are impossible by construction.** Any behaviour of ours that "reimports" is free of
duplication risk, and matching `start` exactly is the only requirement.

### 1.3 `sum` belongs to the integration; the recorder never recomputes it

`StatisticData` is a `TypedDict` where every value field is optional and `sum` is just a float:

```python
class StatisticMixIn(TypedDict, total=False):
    """Mandatory fields for statistic data class."""

    state: float
    sum: float
    min: float
    max: float
    mean: float
    mean_weight: float
```
([`models/statistics.py#L30-L44`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/models/statistics.py#L30-L44))

There is no monotonicity check, no "must start from zero" rule, and no recomputation anywhere:
`_import_statistics_with_session` writes exactly the rows you hand it and touches nothing else
(quoted above). A grep for the only function in the file that rewrites `sum` across a range,
`_adjust_sum_statistics`, finds it defined once and called from exactly one place — `adjust_statistics`
([`statistics.py#L855-L877`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L855-L877),
called at
[`#L3038`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L3038)
and
[`#L3046`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L3046)).

Therefore: **rewriting an earlier day's `sum` and stopping leaves a step for every later row.** The
step is visible because consumers read *differences* of sums, not sums:

```python
            statistics_row["change"] = _sum - prev_sum
            prev_sum = _sum
```
([`statistics.py#L2075-L2088`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2075-L2088))

and the Energy Dashboard asks for exactly that:
`statistics_during_period(..., {"energy": kWh}, {"mean", "change"})`
([`energy/websocket_api.py#L281-L297`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/energy/websocket_api.py#L281-L297)).
A single bad row therefore produces two wrong buckets (one spike, one dip), and a permanent offset if
the step is never healed.

`async_adjust_statistics` is the *intended repair tool for an offset*, not for a rewrite. It shifts
every row at or after a timestamp by a constant, in both tables:

```python
        session.query(table).filter_by(metadata_id=metadata_id).filter(
            table.start_ts >= start_time_ts
        ).update(
            {
                table.sum: table.sum + adj,
            },
            synchronize_session=False,
        )
```
([`statistics.py#L855-L872`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L855-L872),
driven by `adjust_statistics`
[`#L3014-L3054`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L3014-L3054))

Its only caller in core is the admin websocket command `recorder/adjust_sum_statistics`, which exists
so a *user* can correct a wrong reading through the frontend, with unit conversion and an
`unknown_statistic_id` error path
([`websocket_api.py#L500-L563`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/websocket_api.py#L500-L563)).
**We do not need it**: because our reimport range always runs through *yesterday*
(`statistics_mixin.py:193`) and each range recomputes one continuous chain from a single seed, we
rewrite the entire tail and leave no step to adjust away.

### 1.4 How to read back "what sum did I leave off at"

Four candidate readers, and only one pair is usable:

| Function | Signature / behaviour | Verdict for seeding |
|---|---|---|
| `get_last_statistics(hass, number_of_stats, statistic_id, convert_units, types)` — *"Return the last number_of_stats statistics for a statistic_id"* ([`#L2330-L2340`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2330-L2340)) | one `ORDER BY start_ts DESC LIMIT n` on `Statistics` ([`#L2252-L2264`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2252-L2264)) | **The conventional existence probe and the conventional fallback.** Cheapest possible query, but returns the *newest* row, which may be later than the day you are writing. |
| `get_last_short_term_statistics(...)` ([`#L2343-L2353`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2343-L2353)) | identical, against `StatisticsShortTerm` ([`#L2287-L2309`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2287-L2309)) | **Useless for us.** External statistics are only ever written to `Statistics` ([`#L2824`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2824)); this would always return `{}`. Zero core writers call it. |
| `statistic_during_period(hass, start, end, statistic_id, types, units)` ([`#L1831-L1839`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L1831-L1839)) | *singular*; `types: set[Literal["max", "mean", "min", "change"]]` | **Cannot return `sum` at all.** Structurally unusable. Zero core writers call it. |
| `statistics_during_period(hass, start_time, end_time, statistic_ids, period, units, types)` ([`#L2225-L2238`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2225-L2238)) | range scan, `start_ts >= start`, `start_ts < end`, `ORDER BY metadata_id, start_ts` ([`#L1459-L1479`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L1459-L1479)); many IDs in one call | **The conventional baseline read** — used with a one-second window at a known hour, so it returns exactly the row you asked about. |

The core idiom is therefore *both*, in a specific order. `solaredge` states it most clearly, and it is
the pattern we should copy verbatim:

```python
        current_stats = await get_instance(self.hass).async_add_executor_job(
            statistics_during_period,
            self.hass,
            start,
            start + timedelta(seconds=1),
            statistic_ids,
            "hour",
            None,
            {"sum"},
        )
        result = {}
        for statistic_id in statistic_ids:
            if statistic_id in current_stats:
                statistic_sum = current_stats[statistic_id][0]["sum"]
            else:
                # If no statistics found right before start_time,
                # try to get the last statistic but use it only
                # if it's before start_time. This is needed if
                # the integration hasn't run for at least a week.
                last_stat = await get_instance(self.hass).async_add_executor_job(
                    get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
                )
                if (
                    last_stat
                    and last_stat[statistic_id][0]["start"] < start_time.timestamp()
                ):
                    statistic_sum = last_stat[statistic_id][0]["sum"]
                else:
                    # Expected for new installations or if the statistics were cleared,
                    # e.g. from the developer tools
                    statistic_sum = 0.0
```
([`solaredge/coordinator.py#L549-L579`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L549-L579))

**Judging our 25-hour `statistics_during_period` call against that.** Three findings:

1. The *shape* is right: point-in-time read, `"hour"` period, `{"sum"}`, on the recorder executor —
   identical to `opower`, `srp_energy`, `anglian_water`, `mill`, `tibber`, `elvia`, `solaredge`.
2. The *window* is wrong. `0.0` on an empty window (`recorder.py:70`) silently means both "never
   written" and "the last write is older than 25 h", and only the first is safe. Every core writer
   distinguishes those two cases, either with the `get_last_statistics` fallback above or with an
   explicit "fetch all history instead" branch (`tibber` at
   [`coordinator.py#L227-L236`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L227-L236)).
3. The DST justification for 25 hours (`recorder.py:34-35,47-49`) does not hold — see §1.5. One hour is
   always enough for the happy path, and no window at all is enough for the unhappy one.

Also, we issue **six** separate queries (one per statistic, `recorder.py:118-122`) where
`statistics_during_period` accepts a `set[str]` of IDs and `solaredge` passes all of them at once
([`solaredge/coordinator.py#L545`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L545),
`opower` passes four
[`#L280-L288`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L280-L288)).
Each of our queries pays its own `session_scope` and metadata lookup
([`#L2239-L2249`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2239-L2249),
[`#L2110-L2115`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2110-L2115)).
In fairness, the seed-chaining in `async_inject_day` already reduces this to six queries *per range*
rather than per day (`recorder.py:119-122`), so the absolute cost is small.

### 1.5 Hour alignment and DST

The recorder's only requirements on `start` are: timezone-aware, and top of the hour.

```python
    for statistic in statistics:
        start = statistic["start"]
        if start.tzinfo is None or start.tzinfo.utcoffset(start) is None:
            raise HomeAssistantError(
                "Naive timestamp: no or invalid timezone info provided"
            )
        if start.minute != 0 or start.second != 0 or start.microsecond != 0:
            raise HomeAssistantError(
                "Invalid timestamp: timestamps must be from the"
                " top of the hour (minutes and seconds = 0)"
            )

        statistic["start"] = dt_util.as_utc(start)
```
([`statistics.py#L2800-L2812`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2800-L2812))

That is all. There is no requirement that rows be contiguous, that a day contain 24 of them, or even
that they be hourly in spirit: `suez_water` writes one row per *day* at
`dt_util.start_of_local_day(...)`
([`suez_water/coordinator.py#L188-L197`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/suez_water/coordinator.py#L188-L197))
and `ista_ecotrend` writes one per *month*
([`ista_ecotrend/sensor.py#L273-L281`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/sensor.py#L273-L281)),
both into the same hourly `Statistics` table.

**DST is handled at read time, and only for aggregation.** For `period="hour"` there is no alignment
step at all — rows come back as stored
([`#L2128-L2179`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2128-L2179)).
For `"day"`/`"week"`/`"month"`/`"year"` the bounds are converted to local time
([`#L2129-L2138`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2129-L2138))
and the reduction buckets by *local* calendar day:

```python
    def _day_start_end_ts(time: float) -> tuple[float, float]:
        """Return the start and end of the period (day) time is within."""
        start_local = _local_from_timestamp(time).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return (
            start_local.timestamp(),
            (start_local + timedelta(days=1)).timestamp(),
        )
```
([`#L1256-L1290`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L1256-L1290),
used by `_reduce_statistics_per_day`
[`#L1293-L1302`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L1293-L1302)).

So a 23- or 25-hour Amsterdam day is aggregated correctly **from whatever rows exist**. Nothing in
core fabricates the missing hour or de-duplicates the repeated one: **producing the right UTC hour
starts is entirely the importer's job.**

Ours, measured. Running `custom_components/coolblue_energy/statistics.py:48-67` against
`Europe/Amsterdam` for the 2026 transitions (executed locally with CPython `zoneinfo`; the mapping
function is copied verbatim from the module):

| Local date | True local-day length | Labels `00:00`…`23:00` map to | Effect |
|---|---|---|---|
| 2026-10-25 (fall back) | **25 h** (22:00Z 24th → 23:00Z 25th) | 24 distinct UTC slots, 22:00Z → 22:00Z; `01:00`→`23:00Z`, `02:00`→`00:00Z`, `03:00`→`02:00Z` | **01:00Z has no row.** `datetime(..., tzinfo=ZoneInfo(...))` defaults to `fold=0`, i.e. the first (CEST) occurrence of 02:00, so the repeated hour is unreachable by label. If the API sends two `"02:00"` entries, both land on `00:00Z`; the second wins the upsert ([`#L2916-L2917`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2916-L2917)) while `build_stat_data` has already added *both* values to `running_sum` (`external_statistic.py:126-128`) — so the **day total stays correct** and only the hourly distribution is wrong. |
| 2026-03-29 (spring forward) | **23 h** | 24 labels collapse to **23** distinct slots: `02:00` and `03:00` both → `01:00Z` | Only matters if the API emits a `"02:00"` entry for a local hour that does not exist; if it does, its value is folded into the `01:00Z` row's sum and the row itself is overwritten by `03:00`. Same conclusion: total right, one hour's split wrong. |
| 2026-07-01 (normal) | 24 h | 24 distinct slots | correct |

And the fact that kills the 25-hour lookback: on **every** day, including both transitions, the last
row of the previous local day sits exactly one hour before `_day_start_utc(day)` — 22:00Z + 1 h =
23:00Z (26 Oct local midnight), 21:00Z + 1 h = 22:00Z (30 Mar local midnight). DST never widens the
gap, because the rows are UTC-hourly.

---

## 2. Core practice: eleven writers, one shared idiom, no shared code

The eleven statistics writers at `07a2383…` are exactly the set found in
`docs/research/statistics-only-integrations.md` (re-verified with the grep in Method): `elvia`,
`anglian_water`, `ista_ecotrend`, `mill`, `opower`, `solaredge`, `srp_energy`, `suez_water`, `tibber`,
`waterfurnace`, plus the `kitchen_sink` demo. Ten of them write real provider data; each one
implements backfill from scratch.

| Integration (grade) | Seeds the running sum with | Call granularity | First-setup window | Late corrections / gaps | Scheduling |
|---|---|---|---|---|---|
| **[`opower`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L227-L312) (platinum)** | `get_last_statistics(1, …, types=set())` as existence probe ([#L227-L229](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L227-L229)), then `statistics_during_period(start, start+1s, {4 ids}, "hour", None, {"sum"})` with `end=None` retry ([#L274-L299](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L274-L299)) | **one call per statistic per refresh**, carrying the whole fetched range ([#L365-L387](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L365-L387)) | *everything since account activation* — month resolution for all years, day for 3 years, hour for 2 months ([#L516-L599](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L516-L599)) | **re-fetches from `last stat − 30 days`** *"to allow corrections in data from utilities"* ([#L521-L522](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L521-L522), [#L546-L549](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L546-L549)) and re-emits every row after the 30-days-back baseline ([#L328-L331](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L328-L331)) | `DataUpdateCoordinator`, `timedelta(hours=12)` + a dummy listener so it polls without sensors ([#L69-L98](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L69-L98)) |
| **[`ista_ecotrend`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/sensor.py#L228-L296) (gold)** | `get_last_statistics(1, id, False, {"sum"})`; takes `sum` and `end + 1 day` as a cutoff ([#L251-L266](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/sensor.py#L251-L266)) | one call carrying all remaining months ([#L273-L296](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/sensor.py#L273-L296)) | whatever the API returns (monthly history) | **none** — strict `consumptions["date"] > statistics_since` filter ([#L280](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/sensor.py#L280)) | coordinator `timedelta(days=1)` ([`coordinator.py#L37`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/coordinator.py#L37)), driven from the entity's `_handle_coordinator_update` ([`sensor.py#L219-L226`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/sensor.py#L219-L226)) |
| [`elvia`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/elvia/importer.py#L61-L160) (no score) | `get_last_statistics(1, …, {"sum"})` probe, then `statistics_during_period(from_time − 1 h, None, …, {"sum"})` and takes `[0]` ([#L116-L128](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/elvia/importer.py#L116-L128)) | one call, whole payload ([#L147-L159](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/elvia/importer.py#L147-L159)) | **3 years**, fetched as three yearly chunks, tolerating per-year API errors ([#L74-L92](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/elvia/importer.py#L74-L92)) | none (`from_time <= last_stats_time` skip); refuses unverified tail data ([#L107-L114](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/elvia/importer.py#L107-L114)) | `async_track_time_interval(..., timedelta(minutes=60))` ([`__init__.py#L40-L45`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/elvia/__init__.py#L40-L45)) |
| [`suez_water`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/suez_water/coordinator.py#L129-L268) (bronze) | `get_last_statistics(1, id, True, {"sum"})` **only** — no point-in-time read ([#L263-L268](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/suez_water/coordinator.py#L263-L268)) | one call per statistic ([#L229-L245](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/suez_water/coordinator.py#L229-L245)) | `fetch_all_daily_data(since=None)` — everything ([#L154-L158](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/suez_water/coordinator.py#L154-L158)) | none (`data.date <= last_stats` skip, [#L182-L187](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/suez_water/coordinator.py#L182-L187)) | coordinator, `DATA_REFRESH_INTERVAL = timedelta(hours=12)` |
| [`srp_energy`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/srp_energy/coordinator.py#L195-L273) (no score) | `get_last_statistics(**2**, …, {"sum"})` and uses the **older** of the two as the baseline, plus `statistics_during_period(start, start+1s\|None)` ([#L199-L260](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/srp_energy/coordinator.py#L199-L260)) | one call per statistic ([#L354-L362](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/srp_energy/coordinator.py#L354-L362)) | **30 days** ([#L215-L218](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/srp_energy/coordinator.py#L215-L218)) | **yes, by design**: *"We will use the oldest one as the baseline for the sum and re-import the data after it. The last non-zero reported statistic from SRP has potential to get updated as they fill out more data."* ([#L195-L198](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/srp_energy/coordinator.py#L195-L198)) | coordinator, `MIN_TIME_BETWEEN_UPDATES = timedelta(hours=4)` |
| [`anglian_water`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/anglian_water/coordinator.py#L98-L220) (bronze) | probe + `statistics_during_period(start, start+1s\|None)`, **with an explicit fallback to the `get_last_statistics` row** when the point read misses ([#L145-L187](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/anglian_water/coordinator.py#L145-L187)) | one call per meter ([#L220](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/anglian_water/coordinator.py#L220)) | whatever `meter.readings` holds — no explicit window | re-writes the last stored hour when the point read missed (`allow_update_last_stored_hour`, [#L200-L207](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/anglian_water/coordinator.py#L200-L207)); **takes `sum` from the meter's absolute register**, `usage_sum = max(0, read["read"])` ([#L209](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/anglian_water/coordinator.py#L209)) — immune to drift | coordinator, `UPDATE_INTERVAL = timedelta(minutes=60)` |
| [`mill`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/mill/coordinator.py#L83-L165) (no score) | probe, then `statistics_during_period(start_time, None, …, {"sum","state"})` and **subtracts the first row's own state** so it can rewrite it: `_sum = stat["sum"] - stat["state"]` ([#L126-L139](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/mill/coordinator.py#L126-L139)) | one call per device ([#L165](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/mill/coordinator.py#L165)) | `TWO_YEARS_DAYS = 2 * 365` ([#L32](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/mill/coordinator.py#L32), [#L103](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/mill/coordinator.py#L103)) | re-fetches `days since last stat + 2` ([#L110-L121](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/mill/coordinator.py#L110-L121)) and rewrites from the first row inclusive | self-rescheduling to hh:01: `self.update_interval = timedelta(hours=1) + now.replace(minute=1, second=0) - now` ([#L86-L89](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/mill/coordinator.py#L86-L89)) |
| [`tibber`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L157-L276) (no score) | probe, then `statistics_during_period(from_time − 1 h, None, …, {"sum"})`; if the ID is missing, **falls back to refetching 5 years from zero** ([#L217-L236](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L217-L236)) | one call per series ([#L276](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L276)) | `FIVE_YEARS = 5 * 365 * 24` hours ([#L35](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L35), [#L196-L198](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L196-L198)) | comment claims *"We update the statistics with the last 30 days of data to handle corrections in the data"* ([#L203-L206](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L203-L206)) but the loop skips `from_time <= last_stats_time_dt` ([#L251-L255](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L251-L255)), so **existing rows are never corrected** | coordinator, `timedelta(minutes=20)` ([#L133-L138](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/tibber/coordinator.py#L133-L138)) |
| [`solaredge`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L430-L582) (no score) | **one batched** `statistics_during_period(start−1 h, +1 s, {all ids}, "hour", None, {"sum"})` for every statistic at once, with a guarded `get_last_statistics` fallback, else `0.0` ([#L540-L582](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L540-L582)) | one call per equipment ID ([#L534](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L534)) | one week (`TimeUnit.WEEK`) | **yes, deliberately**: *"We fetch last week's data from the API and refresh every 12h so we overwrite recent statistics. This is intended to allow adding any corrected/updated data."* ([#L471-L476](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L471-L476)) | coordinator, `MODULE_STATISTICS_UPDATE_DELAY = timedelta(hours=12)`, dummy listener because there are no sensors ([#L446-L465](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L446-L465)) |
| [`waterfurnace`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/coordinator.py#L147-L412) (bronze) | `get_last_statistics(1, …, {"sum"})` **only**, returning `(start, sum)` ([#L147-L161](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/coordinator.py#L147-L161)) | **one call for the entire collected history** — batches are accumulated in memory, then inserted once ([#L303-L318](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/coordinator.py#L303-L318)) | `BACKFILL_LOOKBACK_DAYS = 395`, walked backwards in `BACKFILL_BATCH_DAYS = 5` chunks with 5-30 s random sleeps, **stopping after `BACKFILL_MAX_EMPTY_DAYS = 15` consecutive empty days** ([#L44-L49](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/coordinator.py#L44-L49), [#L241-L301](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/coordinator.py#L241-L301)) | **explicit gap detection**: `if now - last_dt > BACKFILL_GAP_THRESHOLD` → background gap backfill ([#L362-L370](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/coordinator.py#L362-L370)); dedupes by hour and drops the incomplete current hour ([#L205-L239](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/coordinator.py#L205-L239)); no correction of existing rows (`ts <= last_ts` skip) | coordinator, `ENERGY_UPDATE_INTERVAL = timedelta(hours=2)`, dummy listener ([`const.py#L9`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/const.py#L9), [`coordinator.py#L139-L145`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/waterfurnace/coordinator.py#L139-L145)) |
| `kitchen_sink` | n/a — generates synthetic data | n/a | n/a | n/a | demo integration, not a precedent |

### 2.1 `opower` (platinum) at length

The parts that constitute the convention. Existence probe and baseline read:

```python
            last_stat = await get_instance(self.hass).async_add_executor_job(
                get_last_statistics, self.hass, 1, consumption_statistic_id, True, set()
            )
            if not last_stat:
                _LOGGER.debug("Updating statistic for the first time")
                cost_reads = await self._async_get_cost_reads(
                    account, self.api.utility.timezone()
                )
                cost_sum = 0.0
                compensation_sum = 0.0
                consumption_sum = 0.0
                return_sum = 0.0
                last_stats_time = None
            else:
                ...
                cost_reads = await self._async_get_cost_reads(
                    account,
                    self.api.utility.timezone(),
                    last_stat[consumption_statistic_id][0]["start"],
                )
                if not cost_reads:
                    _LOGGER.debug("No recent usage/cost data. Skipping update")
                    continue
                start = cost_reads[0].start_time
                _LOGGER.debug("Getting statistics at: %s", start)
                # In the common case there should be a previous statistic at start time
                # so we only need to fetch one statistic. If there isn't any, fetch all.
                for end in (start + timedelta(seconds=1), None):
                    stats = await get_instance(self.hass).async_add_executor_job(
                        statistics_during_period,
                        self.hass,
                        start,
                        end,
                        {
                            cost_statistic_id,
                            compensation_statistic_id,
                            consumption_statistic_id,
                            return_statistic_id,
                        },
                        "hour",
                        None,
                        {"sum"},
                    )
                    if stats:
                        break
```
([`opower/coordinator.py#L227-L291`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L227-L291))

The correction window, and the fact that it really *is* a rewrite window — `start_time` is the last
stat's start, and the fetch begins 30 days before it:

```python
    async def _async_get_cost_reads(
        self, account: Account, time_zone_str: str, start_time: float | None = None
    ) -> list[CostRead]:
        """Get cost reads.

        If start_time is None, get cost reads since account activation,
        otherwise since start_time - 30 days to allow corrections in data from utilities

        We read at different resolutions depending on age:
        - month resolution for all years (since account activation)
        - day resolution for past 3 years (if account's read resolution supports it)
        - hour resolution for past 2 months (if account's read resolution supports it)
        """
```
([`opower/coordinator.py#L516-L528`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L516-L528))

Accumulation and emission — note the baseline is the sum at the *30-days-back* row, and every row
after it is re-emitted (and therefore overwritten in the DB):

```python
            for cost_read in cost_reads:
                start = cost_read.start_time
                if last_stats_time is not None and start.timestamp() <= last_stats_time:
                    continue
                ...
                cost_sum += cost_state
                ...
                cost_statistics.append(
                    StatisticData(start=start, state=cost_state, sum=cost_sum)
                )
            ...
            async_add_external_statistics(self.hass, cost_metadata, cost_statistics)
```
([`opower/coordinator.py#L328-L365`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L328-L365))

`opower` also demonstrates the one genuinely awkward operation core has needed here: a *one-time
statistics migration* that splits negative values out of an existing series into a second statistic,
implemented as read-everything-then-rewrite-everything plus a repair issue
([`#L391-L514`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L391-L514)).
There is no core helper for that either.

### 2.2 `ista_ecotrend` (gold) at length

The whole backfill is 45 lines, and it is the minimal form of the pattern — probe, cutoff, accumulate
in a comprehension, single call:

```python
        statistic_id = f"{DOMAIN}:{name}"
        statistics_sum = 0.0
        statistics_since = None

        last_stats = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics,
            self.hass,
            1,
            statistic_id,
            False,
            {"sum"},
        )

        _LOGGER.debug("Last statistics: %s", last_stats)

        if last_stats:
            statistics_sum = last_stats[statistic_id][0].get("sum") or 0.0
            statistics_since = datetime.datetime.fromtimestamp(
                last_stats[statistic_id][0].get("end") or 0, tz=datetime.UTC
            ) + datetime.timedelta(days=1)

        if monthly_consumptions := get_statistics(...):
            statistics: list[StatisticData] = [
                {
                    "start": consumptions["date"],
                    "state": consumptions["value"],
                    "sum": (statistics_sum := statistics_sum + consumptions["value"]),
                }
                for consumptions in monthly_consumptions
                if statistics_since is None or consumptions["date"] > statistics_since
            ]
            ...
            if statistics:
                _LOGGER.debug("Insert statistics: %s %s", metadata, statistics)
                async_add_external_statistics(self.hass, metadata, statistics)
```
([`ista_ecotrend/sensor.py#L247-L296`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/sensor.py#L247-L296))

Worth noting what the gold-graded example *does not* do: no correction handling, no gap repair, no
point-in-time seed read (it trusts the newest row because it only ever appends), and it pins the
statistic ID into config-entry options so an entity rename cannot orphan the series
([`#L231-L245`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/ista_ecotrend/sensor.py#L231-L245)).
The quality scale plainly does not grade backfill sophistication.

### 2.3 Is there a dominant pattern?

**Yes for seeding, yes for call granularity, no for anything else.**

- **Seeding (10/10):** `get_last_statistics(1, …)` as existence probe. 8 of 10 additionally do a
  point-in-time `statistics_during_period` read at a known hour for the baseline sum; only
  `suez_water` and `waterfurnace` seed straight from `get_last_statistics`. **Zero** use a
  multi-hour lookback window like ours, and zero use `get_last_short_term_statistics` or
  `statistic_during_period`.
- **Call granularity (10/10):** one `async_add_external_statistics` per statistic per refresh,
  carrying every row for the range. **Nobody calls it once per day.**
- **First-setup window:** no convention at all — "everything" (`opower`, `suez_water`,
  `ista_ecotrend`), 5 years (`tibber`), 3 years (`elvia`), 395 days with an empty-day stop
  (`waterfurnace`), 2 years (`mill`), 30 days (`srp_energy`), 1 week (`solaredge`), unspecified
  (`anglian_water`). None is user-configurable.
- **Corrections:** three integrations deliberately rewrite a trailing window (`opower` 30 days,
  `solaredge` 1 week, `srp_energy` 2 rows); `mill` rewrites ~2 days as a side effect; the rest never
  correct anything.
- **Gap repair:** only `waterfurnace` detects a gap explicitly; `anglian_water` and `solaredge` have
  fallbacks that keep the sum continuous across one; nobody else does anything.
- **Scheduling:** `DataUpdateCoordinator` with a fixed `update_interval` (9/10), only `elvia` uses
  `async_track_time_interval`. **Nobody uses `async_track_time_change`.** Three writers with no
  sensors of their own register a dummy listener so the coordinator keeps polling — the same trick as
  our `async_add_listener` no-op (`__init__.py:58`).

**Where we agree:** coordinator-driven polling; per-statistic `StatisticData` with a locally
accumulated `sum`; recorder-executor read-back with `"hour"`/`{"sum"}`; deliberate overwrite of a
trailing window (our `RETRY_DAYS`, cf. `solaredge`/`opower`/`srp_energy`); a first-setup window
constant.

**Where we diverge:** (a) the seed read uses a 25-hour window instead of an exact hour plus fallback —
unique to us and wrong; (b) one call per statistic *per day* instead of per range — unique to us and
merely more expensive; (c) six separate seed queries instead of one batched query — `solaredge` shows
the batched form; (d) we expose a **user-triggered reimport action**, which no core writer does — the
closest core analogue is `opower`'s one-shot migration. (d) is a genuine feature, not a divergence to
fix.

---

## 3. Documented conventions and helpers: three negative results

**3.1 No developer-docs page.** The sitemap enumerates 630 URLs. Filtering
`statistic|recorder|energy|database|db-schema` yields exactly six, *all* blog posts:
`2022/09/29/statistics_refactoring`, `2022/11/16/statistics_refactoring`, `2023/01/02/db-schema-v32`,
`2023/04/30/statistics_impossible_values`, `2025/01/31/energy-distance-units`,
`2025/10/16/recorder-statistics-api-changes`. There is **no** `docs/…` page under any name for the
recorder, for statistics, or for importing history. This re-confirms the finding recorded in
`docs/research/statistics-only-integrations.md`, checked again on 2026-07-28.

The nearest documentation is
[`docs/core/entity/sensor` § Long-term Statistics](https://developers.home-assistant.io/docs/core/entity/sensor),
which is entirely about *entity-backed* statistics: it explains `state_class`, `last_reset`,
zero-points, and *"the logic when updating the statistics is to update the sum column with the
difference between the current state and the previous state"*. None of that applies to external
statistics, where the integration supplies `sum` directly.
[`docs/integration_fetching_data`](https://developers.home-assistant.io/docs/integration_fetching_data)
covers coordinators and never mentions statistics.

**3.2 The newest first-party statistics note is still 2025-10-16.** Filtering the sitemap's blog URLs
for anything published after it yields 50 posts, none statistics- or recorder-related (they are MQTT,
frontend, device-tracker, OAuth, etc.). The 2025-10-16 post itself is a deprecation notice:
`unit_class` and `mean_type` become mandatory in **2026.11** for both the Python functions and the
`recorder/import_statistics` WS command. We already pass both
(`external_statistic.py:92-103`, `statistics.py:77-141`), so we are compliant; the
enforcement mechanism is `report_usage(...)` at
[`statistics.py#L2883-L2894`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2883-L2894).

**3.3 No helper, anywhere in core.** Verified by exhaustive grep, not by search index:

- `homeassistant/helpers/**` — **zero** matches for `async_add_external_statistics`,
  `async_import_statistics`, or `external_statistics`.
- `homeassistant/**` — the cumulative-sum grep finds only integration-private functions
  (`suez_water._build_statistics`, `waterfurnace._build_statistics`,
  `huisbaasje._get_cumulative_value`) and two unrelated `overkiz` `_control_backfill` helpers.
- `recorder/services.yaml` registers `purge`, `purge_entities`, `disable`, `enable`, and
  `get_statistics` (read-only). **No import or backfill action.**
- The websocket surface for writing is `recorder/import_statistics`, which is just a thin,
  admin-only, voluptuous-validated wrapper that dispatches to the same two Python functions:

  ```python
      if valid_entity_id(metadata["statistic_id"]):
          async_import_statistics(hass, metadata, stats, _called_from_ws_api=True)
      else:
          async_add_external_statistics(hass, metadata, stats, _called_from_ws_api=True)
  ```
  ([`websocket_api.py#L619-L623`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/websocket_api.py#L619-L623))

  It offers a Python integration nothing extra, and it suppresses the `report_usage` deprecation
  warnings rather than adding capability.

**Conclusion: there is nothing to adopt but an idiom.** A package like our `ha_external_statistics/`
is exactly the thing core has never factored out.

---

## 4. Libraries

### 4.1 `ldotlopez/ha-historical-sensor` (PyPI `homeassistant-historical-sensor`)

Read at [`96e1d70a9e38`](https://github.com/ldotlopez/ha-historical-sensor/tree/96e1d70a9e38). The
whole library is **355 lines** across three files.

| Question | Answer | Evidence |
|---|---|---|
| What is it? | An abstract `SensorEntity` subclass that periodically calls your `async_update_historical()` and writes the result as external statistics | [`sensor.py#L39-L51`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L39-L51) |
| Which API? | `async_add_external_statistics` (not `async_import_statistics`), with a statistic ID derived from the entity ID: `self.entity_id.replace(".", ":", 1)` | [`sensor.py#L28`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L28), [`#L167`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L167), [`#L188`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L188) |
| **Requires entities?** | **Yes, unavoidably** — `class HistoricalSensor(SensorEntity)`, driven from `async_added_to_hass`, and the statistic ID *is* the entity ID | [`sensor.py#L39`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L39), [`#L78-L112`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L78-L112) |
| Solves sum-seeding? | **No.** It reads the last row (`hass_get_last_statistic` → `get_last_statistics(1, …)`) and hands it to you; `async_calculate_statistic_data` **raises `NotImplementedError`** — the accumulation is yours to write | [`helpers.py#L54-L77`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/helpers.py#L54-L77), [`sensor.py#L191-L197`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L191-L197) |
| Solves backfill? | **No.** There is no first-setup window, no day iteration, no gap detection, no reimport. Its only history logic is a hard cutoff that *discards* anything already covered: `cutoff = latest["start"] + 60*60; hist_states = [x for x in hist_states if x.timestamp > cutoff]` — i.e. **existing rows can never be corrected** | [`sensor.py#L155-L160`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L155-L160) |
| Default metadata | `has_sum=False`, `unit_class=None`, `unit_of_measurement=None` — and it only `LOGGER.debug`s a warning when that makes the Energy Dashboard unusable | [`sensor.py#L179-L191`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L179-L191), [`#L86-L102`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L86-L102) |
| Scheduling | `UPDATE_INTERVAL = timedelta(seconds=30)` via `async_track_time_interval`, with `PollUpdateMixin` commented out and the in-code note *"This weird mechanic comes from removed PollUpdateMixin. We need to improve this in future releases"* | [`sensor.py#L40`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L40), [`#L104-L112`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L104-L112), [`#L199-L250`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/homeassistant_historical_sensor/sensor.py#L199-L250) |
| Maintenance | Last commit **2026-04-11**; newest release **`v3.0.0a5`** (2026-04-11) — a pre-release; last non-pre-release on PyPI is `2.0.0` (2025-02-14). 59 stars, 9 open issues | GitHub API `repos/ldotlopez/ha-historical-sensor`, `…/releases`; `pypi.org/pypi/homeassistant-historical-sensor/json` |
| Dependencies / licence | `dependencies = ["importlib-metadata; python_version >= '3.14'"]`, `requires-python = ">=3.14"`; **GPL-3.0** | [`pyproject.toml`](https://github.com/ldotlopez/ha-historical-sensor/blob/96e1d70a9e38/pyproject.toml), repo licence field |

**Would it fit us? No.** It is an entity-first design and we have deliberately no entities
(`docs/adr/0001-statistics-only-no-entities.md`); adopting it would mean re-adding the six sensors the
ADR removed, and it would still leave us writing the sum accumulation, the day loop, the backfill
window, the retry window, and the reimport action ourselves — i.e. everything in
`ha_external_statistics/` except the ~20 lines of `get_last_statistics` wrapper. Its cutoff rule is
also *strictly worse* than ours for a provider that publishes late, because it can never rewrite a
row.

### 4.2 `klausj1/homeassistant-statistics` (HACS `import_statistics`)

The most-used tool in this space (176 stars, MIT, actively pushed 2026-07-27), read at
[`5f46aa5`](https://github.com/klausj1/homeassistant-statistics/tree/5f46aa5790f702ac555f9fa505412a1d5b15123d).
It is **a HACS integration, not a library**: `integration_type: service`, `config_flow: true`,
`single_config_entry: true`, `requirements: ["pandas>=2.0.0"]`
([`manifest.json`](https://github.com/klausj1/homeassistant-statistics/blob/5f46aa5790f702ac555f9fa505412a1d5b15123d/custom_components/import_statistics/manifest.json)).
Its product is user-facing actions that import CSV/TSV/JSON, dispatching to the same two core
functions
([`import_service.py#L265-L290`](https://github.com/klausj1/homeassistant-statistics/blob/5f46aa5790f702ac555f9fa505412a1d5b15123d/custom_components/import_statistics/import_service.py#L265-L290)).
There is nothing importable: no `pyproject.toml` package, no PyPI distribution, and it reaches into a
core private (`from homeassistant.components.recorder.statistics import _statistics_at_time`,
[`delta_database_access.py#L8`](https://github.com/klausj1/homeassistant-statistics/blob/5f46aa5790f702ac555f9fa505412a1d5b15123d/custom_components/import_statistics/delta_database_access.py#L8)).

It is still valuable to us as **independent corroboration of §1.3**. To insert *older* delta data
without breaking the sums that already exist after it, it computes the chain **backwards** from a
newer reference row and then writes a "connection record" to bridge back to the existing series:

```python
    # Work backward from newest to oldest: subtract deltas instead of adding
    ...
    for i, delta_row in enumerate(reversed_rows):
        sum_reference -= delta_row["delta"]
        state_reference -= delta_row["delta"]
```
([`import_service_delta_helper.py#L78-L170`](https://github.com/klausj1/homeassistant-statistics/blob/5f46aa5790f702ac555f9fa505412a1d5b15123d/custom_components/import_statistics/import_service_delta_helper.py#L78-L170))

Somebody else independently concluded that the recorder will not fix your downstream sums, and that
your two options are "rewrite everything after the insertion point" or "work backwards from a fixed
future point". We take the first option, and that is why our reimport is safe.

### 4.3 Other candidates, and the negative result

GitHub repository search (`search/repositories`, queries
`home+assistant+external+statistics+in:name,description,readme` and
`homeassistant+long+term+statistics+import+in:readme`, 15 results each) surfaced no further
libraries. The only on-topic hits were the two above plus
`naevtamarkus/homeassistant-statistics-cli` (an external CLI operating on the recorder DB, not an
integration dependency) and `MyElectricalData/myelectricaldata_import` (a standalone Dockerised
Enedis gateway, not a Python package for integrations). PyPI's own search endpoint returned a bot
challenge rather than results, so this axis rests on the GitHub search plus the exhaustive core grep
(**[INFERENCE]**: a reusable, maintained, HA-statistics-import package with meaningful adoption would
almost certainly be reachable from one of those two searches; I cannot prove exhaustiveness of PyPI).

Decisive corroboration from core itself: **not one of the ten core writers depends on any
third-party statistics-import library.** All ten import directly from
`homeassistant.components.recorder.statistics`. For a HACS integration, a `manifest.json`
`requirements` entry is possible — `import_statistics` ships `pandas>=2.0.0` — but there is no
candidate worth adding one for.

**Verdict: keep `ha_external_statistics/`.** It already contains what every alternative lacks:
sum-seeding, day chaining, a first-setup backfill window, a trailing retry window that *rewrites*
rather than skips, and a reimport entry point — with no entity dependency and no external
requirement.

---

## 5. Optimality verdict: failure modes, judged

| Failure mode | Real for us? | Why, and what decides it |
|---|---|---|
| **Duplicate rows** | **No** | The importer upserts on `(metadata_id, start_ts)` ([`#L2915-L2919`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2915-L2919), [`#L2749-L2762`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2749-L2762)), and the DB layer additionally filters unique-constraint violations ([`#L3003-L3008`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L3003-L3008)). Our 3-day overlap and our reimport are free. |
| **Sum discontinuity when reimporting an older day** | **No** | `async_reimport_statistics` always ends at *yesterday* (`statistics_mixin.py:193`) and `_async_process_day_range` chains one continuous sum from a single seed (`statistics_mixin.py:134-145`), so the whole tail is rewritten. Had we rewritten a *middle* range, the step would be permanent — nothing recomputes ([§1.3](#13-sum-belongs-to-the-integration-the-recorder-never-recomputes-it)). |
| **Seed read returns `0.0` after a gap > 25 h** | **YES — the real bug** | `recorder.py:51-70` cannot distinguish "never written" from "stale". Consequences are permanent: no recomputation ([`#L2915-L2919`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2915-L2919)) and `change = sum − prev_sum` ([`#L2087`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2087)). Triggers: HA down > 25 h, an outage longer than `RETRY_DAYS`, statistics cleared from developer tools (the case `solaredge` names explicitly, [`#L577-L579`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L577-L579)) — and one that needs no outage at all: a mid-range day the provider never published makes `_process_day` return `None` (`coordinator.py:130-134`), which resets the seed (`statistics_mixin.py:142,145`) so the next day queries a window containing only rowless hours. |
| **Sum drift across a failed day** | **Yes, by design, and bounded** | On a failed day the seed is preserved, i.e. the day counts as zero consumption (`statistics_mixin.py:117-121`, in-code rationale at `:152-157`). If the day publishes later *within* `RETRY_DAYS`, the next pass re-injects it and — because the loop is oldest→newest and rewrites every later day in the same range — the chain self-heals. Outside the window the understatement is permanent. `opower` covers 30 days for exactly this reason ([`#L521-L522`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L521-L522)); we cover 3. |
| **Late data corrections from the provider** | **Partly covered** | Any *value change* inside the last 3 days is picked up and overwritten (upsert). Older corrections are invisible to us — same limitation as `elvia`, `tibber`, `ista_ecotrend`, `suez_water`, `waterfurnace`; better than all of them, since they skip rather than overwrite even inside their windows. |
| **Recorder task-queue pressure from 6 × N calls** | **No** | 42 enqueues per backfill and 18 per 6-hour poll (~72/day) against a 65 000-entry ceiling ([`recorder/const.py#L28`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/const.py#L28)). The measurable cost is one forced commit per task ([`core.py#L896-L898`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/core.py#L896-L898)) — 42 instead of 6 during a backfill. Unconventional, not harmful. |
| **DST 23-/25-hour days** | **Yes, cosmetic only** | Verified above: on 2026-10-25 our 24 labels leave `01:00Z` with no row; on 2026-03-29 labels `02:00`/`03:00` collide at `01:00Z`. Because `build_stat_data` accumulates before writing and the DB upserts, the **day total and the sum chain stay correct**; one hour's `state`/`change` is misattributed, twice a year. Core does nothing for importers here beyond the top-of-hour assertion ([`#L2800-L2812`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2800-L2812)); read-time day aggregation uses local boundaries and copes with the row count it finds ([`#L1256-L1302`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L1256-L1302)). |
| **First-setup backfill window too short (7 days)** | **Judgement call, not a bug** | No convention exists (§2.3 — core ranges from 1 week to "everything"). 7 days is the shortest in the field alongside `solaredge`'s 1 week; the reimport action is our escape hatch, which no core writer has. |
| **Cost of the read-back query per day** | **No** | Seed chaining means ~6 queries per *range*, not per day (`recorder.py:119-122`), i.e. ~24 read-only queries/day. Wasteful only in that they could be **one** batched query, as `solaredge` does ([`#L545-L558`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L545-L558)). |
| **Need for `async_adjust_statistics`** | **No** | It shifts every row ≥ a timestamp by a constant ([`#L855-L872`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L855-L872)) and exists for the admin WS command ([`websocket_api.py#L500-L563`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/websocket_api.py#L500-L563)). We never leave an offset to correct. |
| **Rows being purged** | **No** | `purge` deletes short-term statistics only ([`purge.py#L96-L103`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/purge.py#L96-L103), [`#L504-L511`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/purge.py#L504-L511)); external statistics only go to the long-term table ([`statistics.py#L2824`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2824)). |

---

## Recommendation for coolblue_energy

**Our judgement, not core policy.** Core has no policy here: no docs page, no helper, no rule in the
quality scale. Everything below is an engineering call, ranked by payoff. Ordered 1-2 are worth doing;
3-5 are optional; 6 is a decision to *not* do something.

1. **Replace the 25-hour lookback with the `solaredge` two-step read.** *(fixes a real,
   permanent-corruption bug)* In `async_get_last_sum` (`recorder.py:23-70`), read the exact previous
   hour — `statistics_during_period(before_dt − 1 h, before_dt − 1 h + 1 s, ids, "hour", None,
   {"sum"})` — and when that misses, fall back to `get_last_statistics(1, id, True, {"sum"})`,
   accepting its row **only if `row["start"] < before_dt.timestamp()`**, else `0.0`. That is
   `solaredge/coordinator.py` L549-579 verbatim, including its reasoning for the fallback
   ([permalink](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L549-L579)).
   The exact-hour read is safe because the previous row is always exactly one hour before local
   midnight, DST included (verified in §1.5). Delete the DST rationale in the docstring
   (`recorder.py:34-35,47-49`) — it is wrong. Drop the `lookback_hours` parameter with it; nothing
   else passes it.

2. **Batch the six seed reads into one query.** `statistics_during_period` takes
   `statistic_ids: set[str]` and each call costs its own `session_scope` plus metadata lookup
   ([`#L2239-L2249`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2239-L2249)).
   Restructure `async_inject_day` (`recorder.py:116-126`) to collect the statistics missing a seed
   and issue one query for all of them, as `solaredge` does for every equipment ID at once
   ([`#L540-L558`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L540-L558))
   and `opower` does for its four IDs
   ([`#L274-L289`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L274-L289)).
   Naturally combined with item 1.

3. **Widen `RETRY_DAYS`, or say in `CONTEXT.md` why 3 is right.** `const.py:13` says Coolblue
   publishes hours late; the window that actually matters is *how late a correction can arrive*.
   `opower` uses 30 days and documents why
   ([`#L521-L522`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/opower/coordinator.py#L521-L522));
   `solaredge` uses 7
   ([`#L471-L476`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L471-L476));
   `srp_energy` re-imports from two rows back because the provider back-fills
   ([`#L195-L198`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/srp_energy/coordinator.py#L195-L198)).
   Widening costs one API call per extra day per poll and nothing in the recorder (upsert). This is
   the only change that improves *data* accuracy rather than robustness.

4. **Optional: one payload per statistic per range instead of per day.** Every core writer does this
   (§2.3). It would cut a backfill from 42 enqueues and 42 forced commits to 6 and 6
   ([`tasks.py#L28-L37`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/tasks.py#L28-L37),
   [`core.py#L896-L898`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/core.py#L896-L898)).
   **We rank this low and would not do it for performance**: 72 tasks/day is five orders of magnitude
   under the queue ceiling, and per-day flushing means an interrupted range still persists the days it
   completed, which no core writer gets. Do it only if `ha_external_statistics/` is ever extracted for
   ranges of hundreds of days.

5. **Optional: log the DST-day hour collision once.** On the two transition days our label→UTC mapping
   drops or merges one hour (§1.5). The sum stays right, so this is cosmetic; a single `debug` line
   when a range contains a transition would save a future reader the investigation. Do **not** attempt
   `fold=1` reconstruction unless we first confirm the API actually distinguishes the repeated hour —
   the current model has only an `"HH:MM"` label (`statistics.py:48-62`).

6. **Do not adopt a library, and do not add `async_adjust_statistics`.** `ha_external_statistics/`
   already covers strictly more than `ha-historical-sensor` (§4.1) without requiring the entities our
   ADR removed, and `adjust_statistics` solves a problem our whole-tail rewrite prevents (§1.3).

Explicitly **not** recommended, with reasons: switching to `async_import_statistics` (it requires a
real `entity_id` and rejects colon IDs,
[`#L2839`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/recorder/statistics.py#L2839));
using `get_last_short_term_statistics` (always empty for external statistics, §1.4); using
`statistic_during_period` (cannot return `sum`, §1.4); sourcing `sum` from a cumulative meter register
as `anglian_water` does
([`#L209`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/anglian_water/coordinator.py#L209))
— the Coolblue API exposes per-hour deltas only, with no register field
(`custom_components/coolblue_energy/model.py:99-141`).

---

## Confidence and limits

**Verified by reading source at `07a2383bdc4f1a71b0b695e24cbc0c1da823f99b`:**

- `async_add_external_statistics` enqueues an `ImportStatisticsTask`; every task forces a commit
  first; the backlog ceiling is 65 000.
- External statistics are written only to the long-term `Statistics` table, and long-term statistics
  are never purged.
- Re-inserting an existing `(metadata_id, start_ts)` updates every value column in place; duplicates
  are impossible.
- `sum` is an opaque float supplied by the integration; nothing in core recomputes it;
  `_adjust_sum_statistics` has exactly one caller (`adjust_statistics`), reached only from the admin
  websocket command.
- `change` is computed as `sum − previous sum`, and the Energy Dashboard requests `change`.
- The only constraints on `start` are timezone-awareness and top-of-the-hour.
- Read-back: `get_last_short_term_statistics` is useless for external statistics;
  `statistic_during_period` cannot return `sum`; `statistics_during_period` bounds are
  `>= start`, `< end`, ordered by `start_ts`; `period="hour"` performs no alignment.
- The eleven writers and every column of the §2 comparison table.
- No helper for importers anywhere in `homeassistant/helpers/` or `homeassistant/components/recorder/`;
  no import action in `recorder/services.yaml`.
- Zero developer-docs pages on statistics; the newest first-party statistics post is 2025-10-16.
- `ha-historical-sensor` (at `96e1d70a9e38`) requires `SensorEntity`, leaves sum calculation
  abstract, discards overlapping data, is GPL-3.0, and its newest release is a pre-release.
- `import_statistics` (at `5f46aa5`) is a pandas-dependent HACS integration, not a library, and
  computes sums backwards from a newer reference to avoid the discontinuity described in §1.3.
- Our own DST mapping: the exact UTC slots produced for 2026-10-25, 2026-03-29 and 2026-07-01, and
  the fact that the previous row always sits exactly one hour before local midnight. (Computed
  locally with CPython `zoneinfo` using the mapping function copied verbatim from
  `custom_components/coolblue_energy/statistics.py:48-67`; not executed inside Home Assistant.)

**Marked `[INFERENCE]`:**

- **[INFERENCE]** That no other maintained, reusable HA-statistics-import package exists. PyPI's
  search endpoint served a bot challenge, so this rests on two GitHub repository searches plus the
  fact that no core writer depends on such a package.
- **[INFERENCE]** That the >25 h stale-seed path is reachable in *our* deployment specifically. The
  code path is unambiguous, but I did not reproduce it against a live recorder — no test run was
  permitted for this note.
- **[INFERENCE]** That Coolblue's API omits the nonexistent `02:00` on a spring-forward day and
  duplicates `02:00` on a fall-back day. The collision arithmetic is verified; which payload the
  provider actually sends is not, because that needs a live capture across a transition.
- **[INFERENCE]** That a GPL-3.0 requirement would be a problem for adoption. It is a real
  consideration for anything hoping to reach core (Apache-2.0), but I found no core policy text
  stating it, and it is moot given §4.1's other blockers.

**Could not check:**

- The behaviour of any of this against a real database at scale — reads only; no test suite, linter,
  or formatter was run, per the brief.
- Whether core maintainers would *prefer* the per-range or per-day call granularity in review. No
  quality-scale rule, docs page, or PR comment was found addressing statistics backfill at all, which
  is itself the finding in §3.
- `homeassistant/components/opower`'s `_async_maybe_migrate_statistics` end-to-end semantics beyond
  the read/rewrite structure; it was read but not modelled, as it is a one-shot migration unrelated to
  steady-state backfill.
- Line numbers will drift: `dev` moves several times a day. Everything above is pinned to
  `07a2383bdc4f1a71b0b695e24cbc0c1da823f99b`; re-verify blob SHAs before trusting a line number
  against a newer tree.

# The seeding and gap contract of `ha_external_statistics`

**Status.** Decided 2026-07-28 by
[What seeding and gap contract does the library guarantee?](https://github.com/barisdemirdelen/homeassistant-coolblue-energy/issues/21).
This document holds the running-`sum` contract, its derivation rule, and what the library does with a
gap. It is *not* the handoff spec — distribution, testing, the HA support matrix, the backfill loop's
own semantics and both migration paths are other tickets, assembled by
[Assemble the handoff spec](https://github.com/barisdemirdelen/homeassistant-coolblue-energy/issues/26).

Companion document: [the public surface](ha-external-statistics-interface.md), decided by
[What is the library's public surface?](https://github.com/barisdemirdelen/homeassistant-coolblue-energy/issues/20).
§7 below lists the two lines of it this ticket corrects.

Core facts are cited either to `docs/research/backfilling-external-statistics.md`, which pinned them
to a verified core commit, or to the Home Assistant installed in this repo's `.venv`
(**2026.7.4**) where this ticket verified them itself.

---

## 1. The promise

> The seed for a day is the `sum` of the **newest hour strictly before that day's local start**,
> judged across both the recorder and what this run has already written, with **this run's own
> writes winning ties**. `0.0` is used only for a statistic that has never been written at all.

Two consequences the library guarantees from it:

- `sum[n] − sum[n−1] == state[n]` holds for every row the library writes, across day boundaries and
  across gaps alike.
- **A live series never restarts near zero.** That is the whole point: the recorder never recomputes
  a `sum`, and the Energy Dashboard renders `change = sum − prev_sum`, so a single restarted row is
  one enormous negative bar plus a permanent offset that nothing heals
  (`docs/research/backfilling-external-statistics.md` §1.3).

## 2. Why the old read is deleted rather than widened

`async_get_last_sum` looked back a fixed 25 hours and returned `0.0` when it found nothing, which
conflates **"never written"** with **"the last write is older than the window"**. Only the first is
safe. The window's stated justification — covering DST — is false: statistics rows are UTC-hourly, so
the row before a local-midnight boundary sits *exactly* one hour earlier on every day of the year,
both transitions included (verified in `docs/research/backfilling-external-statistics.md` §1.5). One
hour is always enough for the happy path, and no window is enough for the unhappy one.

Deleted with it: the `lookback_hours` parameter, `async_get_last_sum`, `async_inject_day`'s
`seed_sums` argument, and the DST rationale in the docstring.

## 3. Deriving the seed

For a day *D*, let `boundary` be *D*'s local start expressed in UTC, and let `chain_ts` / `chain_sum`
be the UTC hour and cumulative sum of the **last row this run has written** for the statistic in
question (`None` on the first day of the run that produces data).

Evaluated in order, per statistic:

| # | Condition | Seed | Reads |
|---|---|---|---|
| 1 | This run wrote the hour at `boundary − 1h` | `chain_sum` | none |
| 2 | Batched point read returns a row at `boundary − 1h` | that row's `sum` | 1, batched |
| 3 | Point read misses; `get_last_statistics` returns no row | `0.0` | +1 per missing statistic |
| 4 | Point read misses; newest row has `start < boundary` and is **newer** than `chain_ts` | that row's `sum` | +1 per missing statistic |
| 5 | Point read misses; newest row has `start < boundary` and is **at or older than** `chain_ts` | `chain_sum` | +1 per missing statistic |
| 6 | Point read misses; newest row has `start >= boundary` | **raises** — see §4 | +1 per missing statistic |

Case 1 is the rule that makes the whole thing safe without a flush: **the library never reads back a
row it wrote during the same run.** `async_add_external_statistics` only *enqueues* an
`import_statistics` job on the recorder's queue (`recorder/statistics.py`, `async_add_external_statistics`
docstring, HA 2026.7.4), while seed reads run on the recorder's executor — a different thread. A row
written moments ago may not be committed, so reading it back can return the *previous* run's value.
Our own in-memory sum is authoritative for hours we just wrote; the recorder is authoritative for
everything else.

Case 4 is the bug fix. Case 5 is the genuinely-empty gap, where the recorder and the chain agree
anyway. Case 3 is the only route to `0.0`: `get_last_statistics` returning nothing means the
statistic has no rows at all.

The two reads, verbatim from the shape all ten core writers use — `solaredge` batches it exactly this
way for every equipment id at once
([`solaredge/coordinator.py#L540-L582`](https://github.com/home-assistant/core/blob/07a2383bdc4f1a71b0b695e24cbc0c1da823f99b/homeassistant/components/solaredge/coordinator.py#L540-L582)):

```python
# one query for every statistic still needing a seed
statistics_during_period(
    hass,
    boundary - timedelta(hours=1),
    boundary - timedelta(hours=1) + timedelta(seconds=1),
    ids_needing_seed,
    "hour",
    None,
    {"sum"},
)
# per statistic, only for the ones the point read missed
get_last_statistics(hass, 1, statistic_id, False, {"sum"})
```

Both run on the recorder executor via `get_instance(hass).async_add_executor_job`.
`statistics_during_period` accepts a `set[str]`; `get_last_statistics` takes a single id and cannot be
batched, which is why the fallback is per statistic and the point read is not.

`convert_units=False` is deliberate. `convert_units=True` keys its conversion off
`hass.states.get(statistic_id)` (`recorder/statistics.py`, HA 2026.7.4), and a colon-bearing
statistic id can never be a state, so the conversion is the identity for every external statistic.
`False` is the same answer through one fewer code path.

## 4. Error modes

| Condition | Result |
|---|---|
| Newest row for a statistic sits at or after the day being written (case 6) | **Raises immediately, aborting the run.** Same class as #20's out-of-day `ValueError`: deterministic and environmental, not transient. No `ImportReport` change, no per-statistic partial-failure channel. |
| Statistic has no rows at all | Seed `0.0`. Not an error — this is a cold start. |
| Gap in the recorder, of any width | Seeded from the newest row before the day (case 4/5). Never an error, never `0.0`. |

Case 6 is unreachable through the library's own loop: `async_import` and `async_reimport` both end at
*yesterday*, so nothing the library writes is ever ahead of the day in hand. Reaching it means a
backwards clock jump, a hand-edited database, or a second writer on the same statistic id. The
exception message therefore names the statistic id, the offending row's timestamp, and the recovery
in §6 — a loud failure with no stated exit is just a support ticket.

## 5. Gaps

**An unpublished day writes nothing.** `fetch` returning `{}`, or an empty sequence for one
statistic, records the day in `ImportReport.pending` and writes no rows. The library never
zero-fills.

Zero-filling was rejected: it asserts as fact something we do not know, and it is self-sealing — once
written, a synthetic zero row is indistinguishable from a real one. Leaving a hole is what makes late
publication self-healing. When the day appears inside the retry window its rows land, and §3 case 4
picks the truth up for every later day in the same range.

**Residual limitation, documented rather than solved.** If a gap already holds rows from an earlier
run *and* the days **before** the gap are rewritten with changed values, then adopting the gap's
stored sum leaves a step at the pre-gap join, while chaining past it leaves one at the post-gap join.
A chain through rows that can be neither re-fetched nor rewritten cannot be made consistent; only
`recorder/adjust_sum_statistics` could shift them. That is the input this ticket hands
[Does the library ship a repair path for already-corrupted sum series?](https://github.com/barisdemirdelen/homeassistant-coolblue-energy/issues/27).
The common case is unaffected: the retry window exists for late *arrival*, not value churn, so the
gap's stored sums are normally already consistent with the rewritten days before them.

## 6. Recovery

The one persistent failure — a statistic whose newest row sits ahead of yesterday — is cleared with
core's own admin-only `recorder/clear_statistics` websocket command, reachable from
**Developer Tools → Statistics**. It deletes the metadata row and cascades the statistics rows with it
(`recorder/statistics.py::clear_statistics`, queued as `ClearStatisticsTask`, HA 2026.7.4), so the
series becomes genuinely new: the next poll seeds `0.0` per §3 case 3, the backfill window refills it,
and the reimport action refills further back for as long as the provider still serves those days.

The library ships no repair helper of its own; whether the spec should prescribe one is #27.

## 7. Read and write budget

**Reads per run:** one batched point read on the first day that produces data, one more after each
gap, plus at most one `get_last_statistics` per statistic that the point read missed. Every other day
boundary costs zero reads (§3 case 1). Today's code issues six separate reads per range.

**Writes per run:** one `async_add_external_statistics` per statistic per day — ~18 per poll and ~42
per backfill for coolblue, against the recorder queue's 65 000-entry ceiling. This is a **deliberate
divergence from all ten core writers**, which unanimously issue one call per statistic per refresh
carrying the whole range (`docs/research/backfilling-external-statistics.md` §2). Per-day buys flat
memory — one day of readings regardless of range length, so a 365-day reimport does not grow — and
per-day durability: a run that dies mid-range leaves every day it finished correctly written, and the
days it never reached are gaps the retry window heals. No core writer has a user-triggered reimport
over an arbitrary range, which is what makes their budget the wrong one to copy.

**The library never waits on the recorder queue.** Flushing with
`get_instance(hass).async_block_till_done()` before each re-read was considered and rejected: it would
serialise every run against the recorder and buy a dependency on a test-flavoured API, to solve a
problem §3 case 1 solves for free.

## 8. Corrections to the interface document

Two lines of [the public surface](ha-external-statistics-interface.md) are superseded by the above:

- Its **Performance** paragraph says "one more after any hole" without stating what the extra read is
  allowed to conclude. §3 is the precise rule: a gap read may adopt a *recorder* row, never a row
  this run wrote.
- Its error table does not cover case 6. §4 adds it.

Nothing in the five-symbol surface changes: seeds are still never named in the interface, and every
rule here is internal to `StatisticsImporter`.

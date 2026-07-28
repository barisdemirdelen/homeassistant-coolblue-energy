# Coolblue Energy

Imports household electricity and gas data from a Coolblue Energy contract into Home
Assistant, so it can be shown on the Energy Dashboard.

## Language

### The account

**Debtor**:
Coolblue's billing account, identified by a debtor number. One set of credentials resolves
to exactly one debtor.
_Avoid_: customer, user, account

**Location**:
A metered address belonging to a debtor. Readings are always scoped to one location.
_Avoid_: site, address, property, household, meter

### The data

**Reading**:
One hour's measured value for a single energy type at a location on a given day.
_Avoid_: sample, datapoint, measurement, record

**Feed-in**:
Electricity exported from the household to the grid.
_Avoid_: production, export, solar, generation

**Compensation**:
The money credited back for feed-in. Distinct from a cost, and always expressed as a
positive amount.
_Avoid_: credit, refund, feed-in tariff, negative cost

**Publication lag**:
The delay between a day ending and Coolblue making that day's readings available. It is
hours long and not fixed, so a day that is absent is not necessarily a day with no data.
_Avoid_: delay, latency, staleness

### What we write

**External statistic**:
A long-term series in the recorder that this integration owns outright, with no entity
behind it. This is the integration's only output.
_Avoid_: sensor, entity, metric, series

**Statistic ID**:
The identifier naming one external statistic. Its colon prefix is what marks the series as
externally owned rather than entity-backed.
_Avoid_: entity ID, key, name

### Importing

**Backfill**:
The catch-up import of a stretch of history, run once when a config entry is first set up.
A backfill that imports nothing at all has not run: it is re-attempted in full, rather than
leaving the window to the narrower retry days.
_Avoid_: initial sync, history import, seed, bootstrap

**Retry day**:
A recent day re-fetched on every poll because publication lag may have left it empty when
it was last attempted.
_Avoid_: lookback, refresh window, catch-up

**Reimport**:
A user-triggered overwrite of an explicit date range, discarding what is already stored for
those days and recalculating from scratch. The remedy when stored data is wrong rather than
merely missing.
_Avoid_: resync, refresh, repair, reload

# Statistics-only integrations in Home Assistant Core

**Question.** Which Home Assistant *core* integrations write long-term external statistics into the
recorder while registering few or no entities — and how do they declare the Bronze quality-scale
entity rules (`entity-unique-id`, `has-entity-name`, `entity-event-setup`)?

**Why we asked.** `custom_components/coolblue_energy` has `PLATFORMS: list[str] = []`
(`custom_components/coolblue_energy/const.py:8`) and a `sensor.py` containing only a docstring. All
of its output is external statistics with colon-prefixed statistic IDs
(`custom_components/coolblue_energy/statistics.py`, via
`custom_components/coolblue_energy/ha_external_statistics/external_statistic.py:175`). We need to
know whether that shape is precedented in core, and what exemption wording to copy.

---

## Method and provenance

Rather than trust GitHub's code-search index (which is known to be partial), the whole `dev` tree was
downloaded and grepped exhaustively.

- Source: `https://codeload.github.com/home-assistant/core/tar.gz/refs/heads/dev`, fetched
  **2026-07-28**.
- Tree identity **verified**: the local blob SHA of `homeassistant/components/elvia/__init__.py`
  (`143141da8aad36322a1da59b42283c0032bb456a`) matches the GitHub blob SHA for that path at commit
  [`504fddd216b0dee295a58217037a32b7e8d2a166`](https://github.com/home-assistant/core/commit/504fddd216b0dee295a58217037a32b7e8d2a166)
  (dev head, committed 2026-07-28T08:29:17Z). `homeassistant/const.py` reports version
  `2026.8.0.dev0`.
- Search: exhaustive regex grep for `async_add_external_statistics` and `async_import_statistics`
  across `homeassistant/components/**`. Cross-checked against GitHub code search
  (`gh api search/code`), which returned the same integration set.
- All permalinks below are pinned to `504fddd216b0dee295a58217037a32b7e8d2a166`. Line numbers are
  from that commit.

---

## Answer

**Yes — but exactly one, and it carries no quality-scale paperwork.**

1. **`elvia` is a genuine zero-entity, statistics-only core integration.** It has no `PLATFORMS`
   constant, no `async_forward_entry_setups` call, and no platform module of any kind. Its entire
   product is external statistics fed to the Energy Dashboard. It is the direct precedent for the
   *shape* `coolblue_energy` has adopted.
   ([`homeassistant/components/elvia/__init__.py`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/elvia/__init__.py))

2. **`elvia` has no `quality_scale.yaml` and no `quality_scale` key in its manifest**, so it is
   graded "No score". It is grandfathered onto two hassfest allowlists
   (`INTEGRATIONS_WITHOUT_QUALITY_SCALE_FILE` line 304, `INTEGRATIONS_WITHOUT_SCALE` line 1239 of
   [`script/hassfest/quality_scale.py`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/script/hassfest/quality_scale.py)).
   **So `elvia` proves the shape is acceptable, but it is not a source for exemption wording.**

3. **No other core integration is both statistics-writing and entity-free.** The other ten
   statistics writers all forward at least one entity platform. A grep for an empty `PLATFORMS` list
   (`^PLATFORMS(: ...)? = \[\s*\]`) across `homeassistant/components/**` returned **zero matches** —
   core integrations that have no platforms simply omit the constant, as `elvia` does. This is a
   negative result and is reported as such: **there is no core integration that writes external
   statistics, has zero entities, *and* carries a `quality_scale.yaml`.**

4. **For the exemption wording, the precedent set is entity-free integrations generally, not
   statistics writers.** 15 core integrations declare `status: exempt` on all three entity rules, and
   they reach every tier up to **Platinum**. That settles the underlying question: *"no entities" is
   an accepted ground for exempting the Bronze entity rules, and it does not cap your tier.*

5. **Closest analogue for our domain: [`energyid`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/energyid) (silver).**
   An energy integration with no entity platforms at all, whose exemption comment is
   `This integration does not create its own entities.` It moves energy data between HA and a cloud
   service without materialising entities — the same architectural argument we are making. (It
   pushes HA state *out* rather than writing statistics *in*; it does not call the statistics API.
   Verified: no match for `statistic` anywhere under `homeassistant/components/energyid/`.)

6. **Caveat worth internalising.** Each of the three entity-rule pages on
   developers.home-assistant.io states verbatim *"There are no exceptions to this rule."* That text
   contradicts the 190 core integrations that exempt at least one of them. The resolution is
   mechanical, not editorial — see [Rule text](#rule-text) below.

---

## Integrations writing external statistics

Everything in `homeassistant/components/**` at `504fddd` that calls `async_add_external_statistics`
or `async_import_statistics`. `recorder` itself (the API's definition site and its WS handler) and
`energy/websocket_api.py` (which only imports statistics *types*) are excluded.

| Integration | Call site | Entity platforms | Has entities? | `integration_type` | Quality scale |
|---|---|---|---|---|---|
| **[`elvia`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/elvia/importer.py#L147)** | `importer.py:147` | **none — no `PLATFORMS`, no `async_forward_entry_setups`** | **NO** | `service` | *(none — "No score")* |
| [`anglian_water`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/anglian_water/coordinator.py#L220) | `coordinator.py:220` | `_PLATFORMS = [Platform.SENSOR]` (`__init__.py:32`) | yes | `service` | bronze |
| [`ista_ecotrend`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/ista_ecotrend/sensor.py#L296) | `sensor.py:296` | `[Platform.SENSOR]` | yes | `service` | gold |
| [`mill`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/mill/coordinator.py#L165) | `coordinator.py:165` | `[CLIMATE, NUMBER, SENSOR]` | yes | *(unset)* | *(none)* |
| [`opower`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/opower/coordinator.py#L365) | `coordinator.py:365,371,379,387,492` | `[Platform.SENSOR]` | yes | `service` | **platinum** |
| [`solaredge`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/solaredge/coordinator.py#L534) | `coordinator.py:534` | `[Platform.SENSOR]` | yes | `device` | *(none)* |
| [`srp_energy`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/srp_energy/coordinator.py#L354) | `coordinator.py:354,360` | `[Platform.SENSOR]` | yes | `service` | *(none)* |
| [`suez_water`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/suez_water/coordinator.py#L229) | `coordinator.py:229,245` | `[Platform.SENSOR]` | yes | `service` | bronze |
| [`tibber`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/tibber/coordinator.py#L276) | `coordinator.py:276` | `[BINARY_SENSOR, NOTIFY, SENSOR]` | yes | `hub` | *(none)* |
| [`waterfurnace`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/waterfurnace/coordinator.py#L316) | `coordinator.py:316,409` | `[CLIMATE, SENSOR]` | yes | `device` | bronze |
| [`kitchen_sink`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/kitchen_sink/__init__.py#L293) | `__init__.py:293,314,384,…` | 14 platforms | yes | *(unset)* | internal *(demo integration; not a design precedent)* |

Ten of eleven have entities. **One does not: `elvia`.**

### `elvia` in detail

The entire integration is six files:
`__init__.py`, `config_flow.py`, `const.py`, `importer.py`, `manifest.json`, `strings.json`.
([tree](https://github.com/home-assistant/core/tree/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/elvia))

`async_setup_entry` is 30 lines and forwards nothing —
[`__init__.py:19-48`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/elvia/__init__.py#L19-L48):

```python
async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Elvia from a config entry."""
    importer = ElviaImporter(
        hass=hass,
        api_token=entry.data[CONF_API_TOKEN],
        metering_point_id=entry.data[CONF_METERING_POINT_ID],
    )

    async def _import_meter_values(_: datetime | None = None) -> None:
        """Import meter values."""
        try:
            await importer.import_meter_values()
        except ElviaError.ElviaException as exception:
            LOGGER.exception("Unknown error %s", exception)

    try:
        await importer.import_meter_values()
    except ElviaError.ElviaException as exception:
        LOGGER.exception("Unknown error %s", exception)
        return False

    entry.async_on_unload(
        async_track_time_interval(
            hass,
            _import_meter_values,
            timedelta(minutes=60),
        )
    )

    return True
```

Points that map directly onto our situation:

- **Manifest declares `"integration_type": "service"` and `"dependencies": ["recorder"]`**
  ([`manifest.json`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/elvia/manifest.json)).
  Our manifest currently says `"integration_type": "hub"` while also being statistics-only.
- **Colon-prefixed external statistic ID built from the domain**:
  `statistic_id = f"{DOMAIN}:{self.metering_point_id}_consumption"`
  ([`importer.py:64`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/elvia/importer.py#L64)),
  with `source=DOMAIN` at
  [`importer.py:153`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/elvia/importer.py#L153).
  Identical to `coolblue_energy:electricity_consumed`.
- **No `DataUpdateCoordinator` and no `runtime_data`** — it drives itself with
  `async_track_time_interval(..., timedelta(minutes=60))`. Note this would *fail* the Bronze
  `runtime-data` rule if it were graded; it isn't. Do not copy this part.
- **Only `test_config_flow.py` exists** — no `test_init.py`
  ([tests tree](https://github.com/home-assistant/core/tree/504fddd216b0dee295a58217037a32b7e8d2a166/tests/components/elvia)).
  Another reason it is not Bronze-graded.

**The shape was accepted deliberately, and is still maintained.**
[PR #107405 "Add Elvia integration"](https://github.com/home-assistant/core/pull/107405), merged
2024-01-31, shipped with **zero platform files** (the 15 changed files include no `sensor.py`), and
the PR body states the goal plainly:

> Adds a new integration to import meter values from [Elvia](https://www.elvia.no/). […] With this
> integration, all these would be able to see their power consumption in the Energy dashboard (and
> with statistical cards)

Nearly two years later, [PR #159002](https://github.com/home-assistant/core/pull/159002) (merged
2025-12-14) reclassified it to `integration_type: service`, approved by core maintainers @jbouwh and
@ludeeus. Nobody asked it to grow entities. The `elvia` shape is live and endorsed in core today.

---

## Verbatim exemption wording

These are the entity-free core integrations, i.e. every core `quality_scale.yaml` that marks all
three entity rules `exempt`. Of 332 `quality_scale.yaml` files in core, **190 exempt at least one**
of the three entity rules (almost all of those exempt only `entity-event-setup`); **15 exempt all
three**.

| Integration | Tier | Phrasing family |
|---|---|---|
| `azure_storage` | platinum | "This integration does not have entities." |
| `namecheapdns` | platinum | "integration has no entities" |
| `duckdns` | platinum | "integration has no entities" |
| `google_assistant_sdk` | gold | "No entities." |
| `dropbox` | silver | "Integration does not have any entities." |
| **`energyid`** | **silver** | **"This integration does not create its own entities."** |
| `mcp` | silver | "Integration does not have entities." |
| `mcp_server` | silver | "Service does not have entities" |
| `sftp_storage` | silver | "No entities." |
| `splunk` | silver | "Integration does not create entities." |
| `backblaze_b2` | bronze | "This integration does not have entities." |
| `cloudflare_r2` | bronze | "This integration does not have entities." |
| `idrive_e2` | bronze | "This integration does not have entities." |
| `webdav` | bronze | "This integration does not have entities." |
| `spaceapi` | legacy | "This integration has no entities." |

All 15 were independently verified as genuinely entity-free, not merely self-declared: none of them
defines a `PLATFORMS` (or `_PLATFORMS`) constant and none calls `async_forward_entry_setups`
anywhere in its package. Note the tier spread — three are **platinum**. Being entity-free does not
cap an integration's quality scale.

### `energyid` — closest domain analogue (silver)

Source: <https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/energyid/quality_scale.yaml>
(lines 26-34 for the entity rules)

```yaml
  entity-event-setup:
    status: exempt
    comment: This integration does not create its own entities.
  entity-unique-id:
    status: exempt
    comment: This integration does not create its own entities.
  has-entity-name:
    status: exempt
    comment: This integration does not create its own entities.
```

And the adjacent rules the brief asked about, from the same file:

```yaml
  common-modules: done
  appropriate-polling:
    status: exempt
    comment: The integration uses a push-based mechanism with a background sync task, not polling.
  action-setup:
    status: exempt
    comment: The integration does not expose any custom service actions.
  runtime-data: done
```

Note `energyid` marks `common-modules: done` and `runtime-data: done` even with no entities — those
two rules are about module layout (`coordinator.py`/`entity.py`) and `ConfigEntry.runtime_data`, and
remain achievable without entities.

### `dropbox` — most complete "no entities" set (silver)

Source: <https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/dropbox/quality_scale.yaml>
(lines 28-36)

```yaml
  entity-event-setup:
    status: exempt
    comment: Integration does not have any entities.
  entity-unique-id:
    status: exempt
    comment: Integration does not have any entities.
  has-entity-name:
    status: exempt
    comment: Integration does not have any entities.
```

`dropbox` is the only one of the 15 that also exempts `common-modules` on entity grounds:

```yaml
  common-modules:
    status: exempt
    comment: Integration does not have any entities or coordinators.
  appropriate-polling:
    status: exempt
    comment: Integration does not poll.
  action-setup:
    status: exempt
    comment: Integration does not register any actions.
  runtime-data: done
```

### `azure_storage` — platinum, block-scalar style

Source: <https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/azure_storage/quality_scale.yaml>
(lines 28-39)

```yaml
  entity-event-setup:
    status: exempt
    comment: |
      Entities of this integration does not explicitly subscribe to events.
  entity-unique-id:
    status: exempt
    comment: |
      This integration does not have entities.
  has-entity-name:
    status: exempt
    comment: |
      This integration does not have entities.
```

```yaml
  common-modules: done
  appropriate-polling:
    status: exempt
    comment: |
      This integration does not poll.
  action-setup:
    status: exempt
    comment: Integration does not register custom actions.
  runtime-data: done
```

The `webdav` file ([lines 28-39](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/webdav/quality_scale.yaml))
is byte-identical for these six rules. `backblaze_b2`, `cloudflare_r2` and `idrive_e2` use the same
text as plain scalars. Note the grammatical slip *"Entities of this integration **does** not
explicitly subscribe to events"* is copy-pasted verbatim across `azure_storage`, `webdav` and
`idrive_e2` — and it is a slightly odd claim for an integration that has no entities at all.
`dropbox`, `energyid`, `duckdns`, `namecheapdns`, `spaceapi`, `google_assistant_sdk` and `splunk`
give the cleaner answer and reuse the "no entities" reason for all three rules.

### `namecheapdns` / `duckdns` — platinum, terse lowercase style

Source: <https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/namecheapdns/quality_scale.yaml>
(lines 24-32); `duckdns` is identical for these three rules.

```yaml
  entity-event-setup:
    status: exempt
    comment: integration has no entities
  entity-unique-id:
    status: exempt
    comment: integration has no entities
  has-entity-name:
    status: exempt
    comment: integration has no entities
```

Both mark `appropriate-polling: done` (they do poll), `common-modules: done`, `runtime-data: done`.
Their `parallel-updates` exemption is worded on platform rather than entity grounds:

```yaml
  parallel-updates:
    status: exempt
    comment: integration has no entity platforms
```

### `mcp_server` — silver, "service" framing

Source: <https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/mcp_server/quality_scale.yaml>
(lines 28-36)

```yaml
  entity-event-setup:
    status: exempt
    comment: Service does not subscribe to events
  entity-unique-id:
    status: exempt
    comment: Service does not have entities
  has-entity-name:
    status: exempt
    comment: Service does not have entities
```

```yaml
  common-modules:
    status: exempt
    comment: Service does not have entities or coordinators
  appropriate-polling:
    status: exempt
    comment: Service is not polling
  action-setup:
    status: exempt
    comment: Service does not register actions
  runtime-data:
    status: exempt
    comment: No configuration state is used by the integration
```

This is the only one of the 15 that exempts `runtime-data`, and it does so on "no state to store"
grounds, not "no entities" grounds.

### `splunk` — silver, block-scalar, push integration

Source: <https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/splunk/quality_scale.yaml>
(lines 30-41)

```yaml
  entity-event-setup:
    status: exempt
    comment: |
      Integration does not create entities.
  entity-unique-id:
    status: exempt
    comment: |
      Integration does not create entities.
  has-entity-name:
    status: exempt
    comment: |
      Integration does not create entities.
```

Its `appropriate-polling` comment is the best model for an event/schedule-driven integration:

```yaml
  appropriate-polling:
    status: exempt
    comment: |
      Event-driven push integration that listens to state changes, no polling occurs.
```

### `google_assistant_sdk` — gold, minimal style

Source: <https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/google_assistant_sdk/quality_scale.yaml>
(lines 22-30)

```yaml
  entity-event-setup:
    status: exempt
    comment: No entities.
  entity-unique-id:
    status: exempt
    comment: No entities.
  has-entity-name:
    status: exempt
    comment: No entities.
```

### For contrast: statistics writers that *do* have entities

None of these exempt `entity-unique-id` or `has-entity-name` — they mark them `done`, because they
have sensors. Only `entity-event-setup` is exempted, and always on "we don't subscribe" grounds
rather than "we have no entities":

| Integration | `entity-event-setup` comment | Source |
|---|---|---|
| `opower` (platinum) | `The integration does not subscribe to events.` | [quality_scale.yaml](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/opower/quality_scale.yaml) |
| `ista_ecotrend` (gold) | `The integration registers no events.` | [quality_scale.yaml](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/ista_ecotrend/quality_scale.yaml) |
| `waterfurnace` (bronze) | `This integration does not subscribe to events.` | [quality_scale.yaml](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/waterfurnace/quality_scale.yaml) |
| `suez_water` (bronze) | `no subscription to api` | [quality_scale.yaml](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/suez_water/quality_scale.yaml) |
| `anglian_water` (bronze) | *(marked `done`, not exempt)* | [quality_scale.yaml](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/anglian_water/quality_scale.yaml) |

All five mark `common-modules: done`, `appropriate-polling: done` and `runtime-data: done`.

---

## Rule text

### The apparent contradiction

All six rule pages requested carry an identical `## Exceptions` section reading, in full:

> There are no exceptions to this rule.

Verified on each of:

- [`entity-unique-id`](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/entity-unique-id/)
- [`has-entity-name`](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/has-entity-name/)
- [`entity-event-setup`](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/entity-event-setup/)
- [`runtime-data`](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/runtime-data/)
- [`action-setup`](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/action-setup/)
- [`appropriate-polling`](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/appropriate-polling/)
- [`common-modules`](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/common-modules/)

**None of these pages says anything about integrations that provide no entities.** Not one mentions
the case. This is a documentation gap, not a prohibition.

### How the contradiction is actually resolved

The `exempt` mechanism is defined at the *framework* level, not per rule. From
[Integration quality scale § Keeping track of the implemented rules](https://developers.home-assistant.io/docs/core/integration-quality-scale/):

> Integrations that are working towards a higher tier or have a tier, must add a `quality_scale.yaml`
> file to their integration. The purpose of this file is to keep track of the progress of the rules
> that have been implemented **and to keep track of exempted rules and the reason for the
> exemption**. An example of this file looks like this:
>
> ```yaml
> rules:
>   config_flow: done
>   docs_high_level_description:
>     status: exempt
>     comment: This integration does not connect to any device or service.
> ```

(Emphasis added.) Read together: the *rule page's* "Exceptions" section governs whether you may skip
the rule **while still having entities**. It does not govern whether the rule *applies at all*. An
integration with no entities has nothing for `entity-unique-id` to be true or false about, and the
`quality_scale.yaml` `exempt` status with a comment is the documented way to record that.

The enforcement code confirms this reading. In
[`script/hassfest/quality_scale.py`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/script/hassfest/quality_scale.py#L2087-L2109),
every rule in `ALL_RULES` accepts `exempt` with a mandatory comment:

```python
SCHEMA = vol.Schema(
    {
        vol.Required("rules"): vol.Schema(
            {
                vol.Required(rule.name): vol.Any(
                    vol.In(["todo", "done"]),
                    vol.Schema(
                        {
                            vol.Required("status"): vol.In(["todo", "done"]),
                            vol.Required("comment"): str,
                        }
                    ),
                    vol.Schema(
                        {
                            vol.Required("status"): "exempt",
                            vol.Required("comment"): str,
                        }
                    ),
                )
                for rule in ALL_RULES
            }
        )
    }
)
```

And `exempt` counts as satisfying the tier requirement, while skipping any AST validator attached to
the rule — [lines 2214-2242](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/script/hassfest/quality_scale.py#L2214-L2242):

```python
    for rule_name, rule_value in data.get("rules", {}).items():
        status = rule_value["status"] if isinstance(rule_value, dict) else rule_value
        if status not in {"done", "exempt"}:
            continue
        rules_met.add(rule_name)
        if status == "done":
            rules_done.add(rule_name)

    for rule_name in rules_done:
        if (validator := VALIDATORS.get(rule_name)) and (
            errors := validator.validate(config, integration, rules_done=rules_done)
        ):
            ...
    ...
        required_rules = set(SCALE_RULES[scale])
        if missing_rules := (required_rules - rules_met):
```

`rules_met` (which gates the tier) includes `exempt`; `rules_done` (which triggers validators) does
not. **`status: exempt` is a first-class way to satisfy a Bronze rule.** All three entity rules are
Bronze-tier (`entity-event-setup`, `entity-unique-id`, `has-entity-name` in `ALL_RULES`,
`script/hassfest/quality_scale.py:51-53`), as are `action-setup`, `appropriate-polling`,
`common-modules` and `runtime-data`.

Only four Bronze rules have AST validators at all (`config-flow`, `runtime-data`,
`test-before-setup`, `unique-config-entry`), so the three entity rules are honour-system + code
review either way.

### `runtime-data` — the one to be careful with

This rule *does* have a validator, and it is unconditional on entities. From
[`script/hassfest/quality_scale_validation/runtime_data.py`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/script/hassfest/quality_scale_validation/runtime_data.py):

```python
    if not _sets_runtime_data(async_setup_entry, config_entry_argument):
        errors.append(
            "Integration does not set entry.runtime_data in async_setup_entry"
            f"({init_file})"
        )
```

The rule page says:

> The `ConfigEntry` object has a `runtime_data` attribute that can be used to store runtime data.
> […] By using `runtime_data`, we maintain consistency for developers to store runtime data in a
> consistent and typed way.

Having no entities is *not* a reason to exempt this. Of the 15 entity-free integrations, **12 mark
it `done`**, one leaves it `todo` (`spaceapi`), and only two exempt it — each on a "no state to
store" ground, never on an entity ground:

```yaml
  runtime-data:
    status: exempt
    comment: No configuration state is used by the integration
```

<https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/mcp_server/quality_scale.yaml>

```yaml
  runtime-data:
    status: exempt
    comment: |
      Integration has no per-entry runtime state to store. The only data in
      hass.data is a YAML entity filter bridged from async_setup to
      async_setup_entry; no platforms or other code accesses it afterward.
```

<https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/splunk/quality_scale.yaml#L44-L49>

`coolblue_energy` stores a coordinator per entry, so neither escape hatch applies to us.

### `appropriate-polling`

> There is no real definition of what an appropriate polling interval is, as it depends on the
> device or service being polled. […] To give another example, if we poll a cloud service for solar
> panel data where the data is updated every hour. It would not make sense for us to poll every
> minute, as the data will not change between the polls.

Applies normally to a statistics-only integration — statistics importers poll. `duckdns`,
`namecheapdns` and `mcp` (all entity-free) mark it `done`. Exempt it only if you genuinely do not
poll.

### `action-setup`

> We rather prefer integrations to set up their service actions in the `async_setup` method. This
> way we can let the user know why the service action did not work, if the targeted configuration
> entry is not loaded.

Orthogonal to entities. Exempt only if you register no actions. Note `coolblue_energy` ships a
`services.yaml`, so this likely needs to be `done`, not `exempt`.

### `common-modules`

> The majority of new integrations use a coordinator to centralize their data fetching. The
> coordinator should be placed in `coordinator.py`. […] The second common pattern is the base
> entity. […] The base entity should be placed in `entity.py`.

Half of the rule (the coordinator half) applies without entities; 12 of the 15 entity-free
integrations mark it `done`. Only `dropbox`, `mcp_server` and `spaceapi` exempt it, and all three do
so because they have **neither entities nor a coordinator**. An integration with a
`DataUpdateCoordinator` in `coordinator.py` should mark this `done`.

---

## Statistics-specific guidance in the developer docs

**Negative finding: there is no developer-docs page about external statistics.**

- The full <https://developers.home-assistant.io/sitemap.xml> was fetched and filtered for
  `statistic|recorder|energy`. The only hits are five blog posts; there is **no** page under
  `docs/core/` or `docs/api/` covering the recorder statistics API or when to use it.
- GitHub code search over `home-assistant/developers.home-assistant` for
  `async_add_external_statistics` returns **1** result, the October 2025 API-change blog post; a
  search for the phrase `"external statistics"` returns **0**.
- The same searches over `home-assistant/home-assistant.io` return **0** results.

Consequently **no official statement exists telling integrations when to prefer external statistics
over entities.** Anyone claiming HA "requires" entities alongside statistics, or "endorses"
statistics-only integrations, is stating something the docs do not say. The only official artefacts
are:

**1. The sensor docs — entity-centric, and silent on the external path.**
[developers.home-assistant.io/docs/core/entity/sensor/#long-term-statistics](https://developers.home-assistant.io/docs/core/entity/sensor/#long-term-statistics):

> Home Assistant has support for storing sensors as long-term statistics if the entity has the right
> properties. To opt-in for statistics, the sensor must have `state_class` set to one of the valid
> state classes: `SensorStateClass.MEASUREMENT`, `SensorStateClass.TOTAL` or
> `SensorStateClass.TOTAL_INCREASING`. For certain device classes, the unit of the statistics is
> normalized to for example make it possible to plot several sensors in a single graph.

That covers only the *entity-derived* path. It never mentions `async_add_external_statistics`, and
imposes no requirement on integrations that bypass entities.

**2. The API-change blog post** —
[Changes to the recorder statistics API](https://developers.home-assistant.io/blog/2025/10/16/recorder-statistics-api-changes/)
(Erik Montnemery, 2025-10-16). Actionable and deadline-bearing:

> The metadata object passed to the functions `async_import_statistics` and
> `async_add_external_statistics` accepts a `unit_class` that points to the unit converter used for
> unit conversions. If there is no compatible unit converter, `unit_class` should be set to `None`.
> Not specifying the `unit_class` is deprecated and will stop working in Home Assistant Core 2025.11.
>
> The metadata object passed to the functions `async_import_statistics` and
> `async_add_external_statistics` accepts a `mean_type` of type `StatisticMeanType` that specifies
> the type of mean (`NONE`, `ARITHMETIC`, or `CIRCULAR`). The `mean_type` replaces the bool flag
> `has_mean`. Not specifying the `mean_type` is deprecated and will stop working in Home Assistant
> Core 2026.11.

Core enforces these with `report_usage(..., breaks_in_ha_version="2026.11")` at
[`recorder/statistics.py:2884-2895`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/recorder/statistics.py#L2884-L2895).
The comment in the deprecation guard explicitly names custom integrations as the reason it exists:

> ```python
> # Note: This can't happen from the type checker's perspective, but we need
> # to guard against custom integrations that have not been updated to set
> # the unit_class.
> ```

**Action item for us:** confirm `coolblue_energy` sets both `mean_type` and `unit_class` in its
`StatisticMetaData`, or it breaks in HA 2026.11.

**3. The API docstrings — the only normative text distinguishing the two functions.**
[`recorder/statistics.py:2828-2896`](https://github.com/home-assistant/core/blob/504fddd216b0dee295a58217037a32b7e8d2a166/homeassistant/components/recorder/statistics.py#L2828-L2896):

```python
def async_import_statistics(...) -> None:
    """Import hourly statistics from an internal source. ..."""
    if not valid_entity_id(metadata["statistic_id"]):
        raise HomeAssistantError("Invalid statistic_id")
```

```python
def async_add_external_statistics(...) -> None:
    """Add hourly statistics from an external source. ..."""
    # The statistic_id has same limitations as an entity_id, but with a ':' as separator
    if not valid_statistic_id(metadata["statistic_id"]):
        raise HomeAssistantError("Invalid statistic_id")

    # The source must not be empty and must be aligned with the statistic_id
    domain, _object_id = split_statistic_id(metadata["statistic_id"])
    if not metadata["source"] or metadata["source"] != domain:
        raise HomeAssistantError("Invalid source")
```

So `async_import_statistics` **requires** a valid `entity_id` (i.e. an entity must exist for that ID
to be meaningful), whereas `async_add_external_statistics` **requires** a colon-separated
`domain:object_id` and enforces `source == domain`. The API is deliberately split so that data
*without* a backing entity has a first-class path. `coolblue_energy:electricity_consumed` is exactly
the form the second function was built for.

---

## Recommendation for `coolblue_energy`

Grounded in the above; the judgement is ours, the facts are cited.

1. **The shape is legitimate.** `elvia` is a live, maintained core integration with zero entities
   whose only output is external statistics for the Energy Dashboard. Keep `PLATFORMS: list[str] = []`.

2. **Delete `custom_components/coolblue_energy/sensor.py`.** It contains only a docstring, and
   `PLATFORMS` is empty so it is never imported. `elvia` ships no platform module at all. A file that
   exists solely to say "there are no sensors here" is noise. *(Recommendation only — not performed;
   this task was read-only outside this document.)*

3. **Change `"integration_type"` from `"hub"` to `"service"`** in `manifest.json`. `elvia` was
   explicitly moved to `service` in [PR #159002](https://github.com/home-assistant/core/pull/159002)
   for being a cloud data-import integration with no hub semantics. Eight of the eleven statistics
   writers use `service` (six) or `device` (two); only `tibber` uses `hub`, and it genuinely fans
   out to multiple homes. `hub` fits an integration that manages sub-devices, which we do not.

4. **Exemption wording to copy.** `energyid`'s phrasing is the best fit — it is silver, it is an
   energy integration, and "does not create its own entities" is precisely accurate for us:

   ```yaml
     entity-event-setup:
       status: exempt
       comment: This integration does not create its own entities.
     entity-unique-id:
       status: exempt
       comment: This integration does not create its own entities.
     has-entity-name:
       status: exempt
       comment: This integration does not create its own entities.
   ```

   Prefer the same reason for all three rules (as `energyid`, `dropbox`, `duckdns`, `namecheapdns`,
   `google_assistant_sdk`, `splunk` and `spaceapi` do). Avoid the `azure_storage`/`webdav` variant
   *"Entities of this integration does not explicitly subscribe to events"* for `entity-event-setup`:
   it is ungrammatical, and it implies entities we do not have.

   Related rules that follow from the same fact, using established wording:

   ```yaml
     parallel-updates:
       status: exempt
       comment: This integration does not create its own entities.   # energyid
     entity-translations:
       status: exempt
       comment: This integration does not create its own entities.   # energyid
     entity-device-class:
       status: exempt
       comment: This integration does not create its own entities.   # energyid
     icon-translations:
       status: exempt
       comment: This integration does not create its own entities.   # energyid
     devices:
       status: exempt
       comment: The integration does not create any entities, nor does it create devices.  # energyid
   ```

5. **Do NOT exempt these on entity grounds** — they are independent of entities and are marked
   `done` by the entity-free integrations at silver and above:
   - `runtime-data` — has an AST validator; must actually set `entry.runtime_data` in
     `async_setup_entry`.
   - `common-modules` — we have `coordinator.py`, so `done`.
   - `appropriate-polling` — we poll a cloud API, so `done` with a justified interval.
   - `action-setup` — we ship `services.yaml`, so `done`, not exempt.

6. **Verify `mean_type` and `unit_class` are set** in the `StatisticMetaData` built in
   `custom_components/coolblue_energy/ha_external_statistics/external_statistic.py`. Both become
   mandatory in HA Core 2026.11.

---

## Confidence and limits

- Everything above about core source is **verified** against a cryptographically identified tree
  (`504fddd216b0dee295a58217037a32b7e8d2a166`) rather than a search index.
- Everything about the developer docs is **verified** by reading the pages directly (they serve
  `text/markdown` via content negotiation).
- The claim *"`elvia` is the only zero-entity statistics writer in core"* is scoped to integrations
  calling `async_add_external_statistics` / `async_import_statistics` directly. An integration that
  wrote statistics through some other path (e.g. a library reaching into the recorder) would not be
  caught. No such path is known to exist. **[INFERENCE]**
- The reading in [Rule text](#rule-text) — that "There are no exceptions to this rule" scopes to
  integrations that *have* entities — is an inference reconciling the rule pages with 190 core
  integrations and the hassfest schema. No HA document states it explicitly. **[INFERENCE]**
- The recommendations in the final section are our judgement, not core policy.
- Not checked: whether the HA core team would accept a statistics-only integration submitted *today*
  at Bronze. `elvia` was merged 2024-01-31, before the current quality scale was rolled out, and has
  never been graded. No core integration has yet been graded Bronze-or-above while being both
  entity-free and statistics-writing, so this specific combination is **unprecedented at Bronze**,
  even though each half of it is well precedented. **[INFERENCE]**

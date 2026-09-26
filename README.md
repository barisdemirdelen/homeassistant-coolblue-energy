# Coolblue Energy — Home Assistant Integration

[![hacs][hacs-badge]][hacs-url]
[![release][release-badge]][release-url]
![downloads][downloads-badge]

A custom Home Assistant integration that pulls your **Coolblue Energy** contract data
into the [Energy Dashboard](https://www.home-assistant.io/docs/energy/).

Because the Coolblue Energy portal only exposes data for the **previous day**, this
integration injects historical hourly readings directly into the HA recorder as
[external statistics](https://developers.home-assistant.io/docs/core/entity/sensor/#long-term-statistics)
rather than exposing live sensors — giving you accurate kWh/m³ history graphs without
needing a P1 dongle.

---

## Features

| What                       | Detail                                              |
|----------------------------|-----------------------------------------------------|
| ⚡ Electricity consumed     | Hourly kWh drawn from the grid                      |
| ☀️ Electricity returned     | Hourly kWh fed back into the grid                   |
| 🔥 Gas consumed            | Hourly m³                                           |
| 💶 Electricity cost        | Hourly € for what was consumed                      |
| 💶 Feed-in compensation    | Hourly € paid back for what was returned            |
| 💶 Gas cost                | Hourly € for gas                                    |
| 📅 7-day backfill          | A week of readings backfilled on first setup        |
| 🔄 Refresh interval        | Every 6 hours                                       |

All six are **external statistics**, not sensors — see
[Statistics, not entities](#statistics-not-entities).

---

## Requirements

- Home Assistant **≥ 2026.2.3**
- A [Coolblue Energy](https://www.coolblue.nl/energie) electricity and/or gas contract
- Your Coolblue account **e-mail address** and **password**

---

## Installation

### Via HACS (recommended)

1. Open HACS → **Integrations** → ⋮ → **Custom repositories**
2. Add `https://github.com/barisdemirdelen/homeassistant-coolblue-energy` with category **Integration**
3. Search for **Coolblue Energy** and click **Download**
4. Restart Home Assistant

### Manual

1. Copy the `custom_components/coolblue_energy` folder into your HA
   `config/custom_components/` directory
2. Restart Home Assistant

---

## Configuration

1. Go to **Settings → Devices & Services → Add integration**
2. Search for **Coolblue Energy**
3. Enter your Coolblue **e-mail address** and **password**

The integration performs an OIDC login to `accounts.coolblue.nl`, fetches your
debtor number and location ID, and begins the back-fill immediately.

### Re-authentication

If your Coolblue password changes, the next poll fails to log in and Home Assistant asks you
for the new one. A repair titled **Authentication expired for Coolblue Energy (debtor …)**
appears under **Settings → System → Repairs**, and the entry is flagged **Attention required**
with a **Reconfigure** button on the integration page. Either one opens a single-field form
asking for the current password; the e-mail address is not re-entered.

---

## Statistics, not entities

This integration creates **no entities and no device**. Its entire output is the six
[external statistics](https://developers.home-assistant.io/docs/core/entity/sensor/#long-term-statistics)
listed below, written straight into the recorder — long-term hourly series the integration
owns outright, with nothing behind them. That is a deliberate decision — six sensors did exist
once and were dropped; [ADR 0001](docs/adr/0001-statistics-only-no-entities.md) records why.

What that means in practice:

- ✅ **Energy Dashboard.** The statistics are selectable as consumption, return, cost, and
  compensation sources, with full hourly history.
- ✅ **Anything else that takes a statistic ID** — the statistic card, the statistics graph
  card, **Developer tools → Statistics**.
- ❌ **No `sensor.*` to template against.** There is no state for `{{ states(...) }}` to read,
  no entity to trigger an automation on, and nothing to build a template sensor from.
- ❌ **No device page, no entity list.** An entry that shows no devices and no entities is this
  integration working correctly, not a failed setup. The evidence it runs is data arriving in
  the Energy Dashboard, plus the log.

---

## Energy Dashboard

After the first successful data fetch, navigate to **Settings → Dashboards → Energy**
and add the statistics injected by this integration:

| Statistic ID                                        | Use for                          |
|-----------------------------------------------------|----------------------------------|
| `coolblue_energy:electricity_consumed`              | Grid consumption                 |
| `coolblue_energy:electricity_returned`              | Return to grid (feed-in)         |
| `coolblue_energy:gas_consumed`                      | Gas consumption                  |
| `coolblue_energy:electricity_cost`                  | Electricity consumption cost (€) |
| `coolblue_energy:electricity_returned_compensation` | Feed-in compensation (€)         |
| `coolblue_energy:gas_cost`                          | Gas cost (€)                     |

The integration injects cumulative hourly sums — the Energy Dashboard will display
them as daily and monthly totals.

### Step-by-step setup

#### Electricity grid

1. Under **Electricity grid**, click **Add consumption** and select
   `coolblue_energy:electricity_consumed` (labelled *Coolblue Electricity Consumed*).
2. Click **Add return** and select `coolblue_energy:electricity_returned`
   (labelled *Coolblue Electricity Returned*).
3. For **Cost tracking**, choose **Use an entity tracking the total costs** and select
   `coolblue_energy:electricity_cost` (consumption cost only).
4. For **Export compensation**, choose **Use an entity tracking the total compensation** and select
   `coolblue_energy:electricity_returned_compensation` (the compensation earned for
   electricity returned to the grid). Only relevant if you feed in, e.g. with solar panels.

#### Gas

1. Under **Gas consumption**, click **Add gas source** and select
   `coolblue_energy:gas_consumed` (labelled *Coolblue Gas Consumed*).
2. For **Cost**, choose **Use an entity tracking the total costs** and select
   `coolblue_energy:gas_cost`.

> **Note:** Data is available from the day _after_ your contract start date.
> The Coolblue portal only publishes data for **yesterday**, so today's usage
> will appear tomorrow.

---

## Actions

### `coolblue_energy.reimport_statistics`

Re-fetches and re-injects all hourly statistics for one debtor, from a given date through
yesterday. Use this to fix gaps, negative spikes, or other artefacts in the Energy Dashboard
(e.g. after a prolonged HA downtime or an API outage).

| Field             | Type     | Required | Description                                  |
|-------------------|----------|----------|----------------------------------------------|
| `config_entry_id` | `string` | ✅        | The Coolblue Energy debtor to reimport       |
| `start_date`      | `date`   | ✅        | First day to reimport (format: `YYYY-MM-DD`) |

A reimport overwrites stored history, so it **must** name the debtor it acts on. There is no
"leave it blank to do every account": an identifier that does not resolve to a currently
loaded debtor is rejected with an error naming that identifier, and nothing is reimported.

In **Developer tools → Actions** the debtor is a dropdown listing your entries by title —
`Coolblue Energy (debtor 00844083)` — so there is no id to type. The YAML below is what that
dropdown produces: `config_entry_id` is Home Assistant's own generated id for the entry, not
your debtor number.

**Example — reimport the last 30 days:**

```yaml
action: coolblue_energy.reimport_statistics
data:
  config_entry_id: 01KYMDY4DSC63EVMGD8J0XPPHZ
  start_date: "2026-03-05"
```

> The action is registered when the integration loads, so it is present in the UI even when no
> entry is. After the reimport finishes the coordinator publishes the reimported data, so the
> Energy Dashboard updates without a restart.

---

## Removing the integration

Removing this integration is two separate things: stopping the imports, and deleting what was
already imported. **Deleting the config entry does not delete the statistics.** They live in
the recorder, no entity owns them, and Home Assistant never purges long-term statistics on its
own — so they keep appearing in the Energy Dashboard until you delete them yourself.

1. **Detach the statistics from the Energy Dashboard.**
   Go to **Settings → Dashboards → Energy** and remove the Coolblue sources you added under
   *Electricity grid* and *Gas consumption*.
2. **Delete the config entry.**
   Go to **Settings → Devices & Services → Coolblue Energy**, then ⋮ on the entry → **Delete**.
   Polling stops and the network session closes immediately; no restart needed.
3. **Delete the imported statistics** — *this is the step that removes the data, and it is
   permanent.* Go to **Developer tools → Statistics**, search for `coolblue_energy`, turn on
   selection mode, tick the six `coolblue_energy:*` rows, then **Delete selected statistics**
   and confirm. Skip this step to keep the history without the integration. Once the config
   entry is gone, `reimport_statistics` cannot bring the data back — you would have to add the
   integration again first.
4. **Uninstall the files.**
   HACS → **Coolblue Energy** → ⋮ → **Remove**, or delete `custom_components/coolblue_energy`
   for a manual install. Restart Home Assistant.

---

## Development

Everything runs through [uv](https://docs.astral.sh/uv/). Never invoke `python`, `pip`,
`pytest`, `ruff`, or `ty` directly — they resolve outside the project venv.

```bash
uv sync                    # create the venv and install dependencies
uv run ruff check .        # lint
uv run ruff format <path>  # format what you touched
uv run ty check            # type check
uv run pytest              # tests
```

See [AGENTS.md](AGENTS.md) for the full toolchain and conventions.

---

## Limitations

- Data is only available for the **previous day**; real-time readings are not possible
- Requires a Coolblue Energy **contract** (electricity and/or gas)
- The integration uses the Coolblue portal's undocumented internal API; changes to
  the portal may break it until an update is released

---

## License

[MIT](LICENSE)

---

## Disclaimer

> ⚠️ **This project was developed with heavy AI assistance.**
> The code has been reviewed and tested by the author, but may contain mistakes or
> security issues. Use at your own risk. This is not an official Coolblue product and
> is not affiliated with or endorsed by Coolblue B.V.


<!-- Badges -->

[hacs-url]: https://github.com/hacs/integration

[hacs-badge]: https://img.shields.io/badge/hacs-default-orange.svg?style=flat-square

[release-url]: https://github.com/barisdemirdelen/homeassistant-coolblue-energy/releases

[release-badge]: https://img.shields.io/github/v/release/barisdemirdelen/homeassistant-coolblue-energy?style=flat-square

[downloads-badge]: https://img.shields.io/github/downloads/barisdemirdelen/homeassistant-coolblue-energy/total?style=flat-square

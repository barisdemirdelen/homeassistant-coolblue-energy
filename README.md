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

| What                            | Detail                                          |
|---------------------------------|-------------------------------------------------|
| ⚡ Electricity consumed          | Hourly kWh, injected into Energy Dashboard      |
| ☀️ Electricity returned (solar) | Hourly kWh, injected into Energy Dashboard      |
| 🔥 Gas consumed                 | Hourly m³, injected into Energy Dashboard       |
| 💰 Spot price                   | Last settled hourly electricity price (€/kWh)   |
| 💶 Daily electricity cost       | Total electricity cost for yesterday (€)        |
| 💶 Daily gas cost               | Total gas cost for yesterday (€)                |
| 📅 7-day back-fill              | History injected automatically on first install |
| 🔄 Refresh interval             | Every 6 hours                                   |

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

If your password changes, HA will prompt you to re-authenticate. Navigate to
**Settings → Devices & Services → Coolblue Energy → Re-configure** and enter your
new password.

---

## Energy Dashboard

After the first successful data fetch, navigate to **Settings → Dashboards → Energy**
and add the statistics injected by this integration:

| Statistic ID                    | Use for                |
|---------------------------------|------------------------|
| `coolblue:electricity_consumed` | Grid consumption       |
| `coolblue:electricity_returned` | Return to grid (solar) |
| `coolblue:gas_consumed`         | Gas consumption        |

The integration injects cumulative hourly sums — the Energy Dashboard will display
them as daily and monthly totals.

> **Note:** Data is available from the day _after_ your contract start date.
> The Coolblue portal only publishes data for **yesterday**, so today's usage
> will appear tomorrow.

---

## Sensors

Six sensor entities are created under the **Coolblue Energy** device:

| Entity                                    | Unit  | Description                               |
|-------------------------------------------|-------|-------------------------------------------|
| `sensor.yesterday_s_electricity`          | kWh   | Total electricity consumed yesterday      |
| `sensor.yesterday_s_electricity_returned` | kWh   | Total solar production returned yesterday |
| `sensor.yesterday_s_gas`                  | m³    | Total gas consumed yesterday              |
| `sensor.spot_price`                       | €/kWh | Last settled spot price of the day        |
| `sensor.daily_electricity_cost`           | €     | Total electricity cost yesterday          |
| `sensor.daily_gas_cost`                   | €     | Total gas cost yesterday                  |

These sensors show **yesterday's** totals and update every 6 hours. They use
`state_class: measurement` so that HA's recorder does not create conflicting
statistics alongside the external statistics injected for the Energy Dashboard.

---

## Development

```bash
# Create virtual environment and install dependencies
uv sync

# Run tests
pytest
```

---

## Limitations

- Data is only available for the **previous day**; real-time readings are not possible
- Requires a Coolblue Energy **contract** (electricity and/or gas)
- The integration scrapes the Coolblue portal's Next.js server actions; changes to
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

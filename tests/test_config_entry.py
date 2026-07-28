"""Config entry lifecycle, driven through Home Assistant's own machinery.

This is the real-Home-Assistant seam: a config entry is set up by
``hass.config_entries``, not by calling ``async_setup_entry`` directly, so the
state the entry reaches is Home Assistant's verdict rather than ours.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
import voluptuous as vol
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ServiceValidationError
from homeassistant.setup import async_setup_component
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.coolblue_energy.const import (
    ATTR_CONFIG_ENTRY_ID,
    ATTR_START_DATE,
    DOMAIN,
    SCAN_INTERVAL,
    SERVICE_REIMPORT_STATISTICS,
)

# One poll of the retry window costs three API calls per day (electricity, gas,
# costs), so a single scheduled refresh is exactly this many calls.
_CALLS_PER_POLL = 3 * 3


async def _setup(
    hass: HomeAssistant, entry: MockConfigEntry, client: AsyncMock
) -> None:
    """Set the entry up through Home Assistant with *client* as its API client."""
    with patch("custom_components.coolblue_energy.ApiClient", return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()


async def _advance_one_interval(hass: HomeAssistant) -> None:
    """Let the clock reach the next scheduled poll and run whatever it triggers."""
    async_fire_time_changed(hass, dt_util.utcnow() + SCAN_INTERVAL)
    # A scheduled coordinator refresh runs as a background task on the entry, so
    # the default block-till-done would return before it has fetched anything.
    await hass.async_block_till_done(wait_background_tasks=True)


async def test_setup_entry_reaches_loaded_state(
    # ``recorder_mock`` must be ordered before ``enable_custom_integrations``:
    # the integration declares a recorder dependency, and setting the custom
    # integration up first leaves the recorder unavailable to it.
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> None:
    """Home Assistant sets the entry up and reports it loaded."""
    config_entry.add_to_hass(hass)

    await _setup(hass, config_entry, mock_api_client)

    assert config_entry.state is ConfigEntryState.LOADED


async def test_loaded_entry_polls_on_its_own_schedule(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> None:
    """The entry keeps fetching once the scan interval elapses."""
    config_entry.add_to_hass(hass)
    await _setup(hass, config_entry, mock_api_client)
    after_setup = mock_api_client.get_hourly_energy.call_count

    await _advance_one_interval(hass)

    assert mock_api_client.get_hourly_energy.call_count == after_setup + _CALLS_PER_POLL


async def test_unload_stops_polling_and_closes_the_session(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> None:
    """Unloading closes the network session and leaves no timer behind."""
    config_entry.add_to_hass(hass)
    await _setup(hass, config_entry, mock_api_client)

    assert await hass.config_entries.async_unload(config_entry.entry_id)
    await hass.async_block_till_done()

    assert config_entry.state is ConfigEntryState.NOT_LOADED
    mock_api_client.close.assert_awaited_once()

    after_unload = mock_api_client.get_hourly_energy.call_count
    await _advance_one_interval(hass)
    assert mock_api_client.get_hourly_energy.call_count == after_unload


async def test_reload_does_not_double_the_poll_rate(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> None:
    """After a reload exactly one schedule is live, not the old one as well."""
    config_entry.add_to_hass(hass)
    await _setup(hass, config_entry, mock_api_client)

    with patch(
        "custom_components.coolblue_energy.ApiClient", return_value=mock_api_client
    ):
        assert await hass.config_entries.async_reload(config_entry.entry_id)
        await hass.async_block_till_done()

    after_reload = mock_api_client.get_hourly_energy.call_count
    await _advance_one_interval(hass)

    assert (
        mock_api_client.get_hourly_energy.call_count == after_reload + _CALLS_PER_POLL
    )


async def test_reimport_action_is_registered_without_a_loaded_entry(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
) -> None:
    """The action exists as soon as the integration is set up, entry or not."""
    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    assert hass.services.has_service(DOMAIN, SERVICE_REIMPORT_STATISTICS)


async def test_reimport_requires_a_target(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> None:
    """Omitting the target is rejected outright — no fan-out across debtors."""
    config_entry.add_to_hass(hass)
    await _setup(hass, config_entry, mock_api_client)
    before = mock_api_client.get_hourly_energy.call_count

    with pytest.raises(vol.Invalid):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_REIMPORT_STATISTICS,
            {ATTR_START_DATE: "2026-01-01"},
            blocking=True,
        )

    assert mock_api_client.get_hourly_energy.call_count == before


async def test_reimport_with_unresolvable_target_raises_and_imports_nothing(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> None:
    """A target that resolves to nothing is an error naming it, not a fan-out."""
    config_entry.add_to_hass(hass)
    await _setup(hass, config_entry, mock_api_client)
    before = mock_api_client.get_hourly_energy.call_count

    with pytest.raises(ServiceValidationError, match="no-such-entry"):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_REIMPORT_STATISTICS,
            {
                ATTR_CONFIG_ENTRY_ID: "no-such-entry",
                ATTR_START_DATE: "2026-01-01",
            },
            blocking=True,
        )

    assert mock_api_client.get_hourly_energy.call_count == before


async def test_reimport_against_an_unloaded_entry_is_refused(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> None:
    """An entry that exists but is not loaded has no coordinator to reimport with."""
    config_entry.add_to_hass(hass)
    await _setup(hass, config_entry, mock_api_client)
    assert await hass.config_entries.async_unload(config_entry.entry_id)
    await hass.async_block_till_done()
    before = mock_api_client.get_hourly_energy.call_count

    with pytest.raises(ServiceValidationError, match=config_entry.entry_id):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_REIMPORT_STATISTICS,
            {
                ATTR_CONFIG_ENTRY_ID: config_entry.entry_id,
                ATTR_START_DATE: "2026-01-01",
            },
            blocking=True,
        )

    assert mock_api_client.get_hourly_energy.call_count == before


async def test_reimport_runs_against_the_named_entry(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> None:
    """A resolvable target reimports that entry's history."""
    config_entry.add_to_hass(hass)
    await _setup(hass, config_entry, mock_api_client)
    before = mock_api_client.get_hourly_energy.call_count
    yesterday = dt_util.now().date() - timedelta(days=1)

    await hass.services.async_call(
        DOMAIN,
        SERVICE_REIMPORT_STATISTICS,
        {
            ATTR_CONFIG_ENTRY_ID: config_entry.entry_id,
            ATTR_START_DATE: yesterday.isoformat(),
        },
        blocking=True,
    )

    # One day (yesterday through yesterday) × electricity, gas, costs.
    assert mock_api_client.get_hourly_energy.call_count == before + 3

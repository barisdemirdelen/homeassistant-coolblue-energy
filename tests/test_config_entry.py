"""Config entry lifecycle, driven through Home Assistant's own machinery.

This is the real-Home-Assistant seam: a config entry is set up by
``hass.config_entries``, not by calling ``async_setup_entry`` directly, so the
state the entry reaches is Home Assistant's verdict rather than ours.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.coolblue_energy.const import (
    CONF_DEBTOR_ID,
    CONF_LOCATION_ID,
    DOMAIN,
)

from .conftest import DEBTOR_ID, LOCATION_ID

ENTRY_DATA = {
    CONF_EMAIL: "user@example.com",
    CONF_PASSWORD: "hunter2",
    CONF_DEBTOR_ID: DEBTOR_ID,
    CONF_LOCATION_ID: LOCATION_ID,
}


@pytest.fixture(autouse=True)
def patch_get_instance():
    """Neutralise the mock-harness recorder patch from ``conftest``.

    Tests in this module run against the recorder the ``recorder_mock`` fixture
    starts, so ``get_instance`` must resolve to that instance.
    """
    return


@pytest.fixture
def config_entry() -> MockConfigEntry:
    """A Coolblue Energy config entry with credentials already resolved."""
    return MockConfigEntry(
        domain=DOMAIN,
        title="Coolblue Energy",
        data=ENTRY_DATA,
        unique_id=DEBTOR_ID,
    )


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

    with patch(
        "custom_components.coolblue_energy.ApiClient", return_value=mock_api_client
    ):
        assert await hass.config_entries.async_setup(config_entry.entry_id)
        await hass.async_block_till_done()

    assert config_entry.state is ConfigEntryState.LOADED

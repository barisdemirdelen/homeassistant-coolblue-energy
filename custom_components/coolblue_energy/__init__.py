"""Coolblue Energy integration."""

from __future__ import annotations

import voluptuous as vol
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType

from .api_client import ApiClient
from .const import (
    ATTR_CONFIG_ENTRY_ID,
    ATTR_START_DATE,
    DOMAIN,
    SERVICE_REIMPORT_STATISTICS,
)
from .coordinator import CoolblueConfigEntry, CoolblueCoordinator, CoolblueRuntimeData

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

_REIMPORT_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_CONFIG_ENTRY_ID): cv.string,
        vol.Required(ATTR_START_DATE): cv.date,
    }
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register the integration's actions, whether or not an entry is loaded."""
    hass.services.async_register(
        DOMAIN,
        SERVICE_REIMPORT_STATISTICS,
        _async_reimport_statistics,
        schema=_REIMPORT_SCHEMA,
    )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: CoolblueConfigEntry) -> bool:
    """Set up Coolblue Energy from a config entry."""
    client = ApiClient(entry.data[CONF_EMAIL], entry.data[CONF_PASSWORD])
    # Registered before the first refresh so a failed setup, which Home Assistant
    # retries with a fresh client, does not leak this one's session.
    entry.async_on_unload(client.close)

    coordinator = CoolblueCoordinator(hass, entry, client)
    await coordinator.async_config_entry_first_refresh()

    entry.runtime_data = CoolblueRuntimeData(coordinator=coordinator, client=client)

    # The integration creates no entities (ADR 0001), and a coordinator with no
    # listeners never reschedules itself. This dummy listener is what keeps the
    # six-hour poll running; unloading the entry removes it again.
    entry.async_on_unload(coordinator.async_add_listener(lambda: None))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: CoolblueConfigEntry) -> bool:
    """Unload a config entry.

    Polling and the network session are both torn down by the callbacks
    registered with ``entry.async_on_unload`` during setup.
    """
    return True


async def _async_reimport_statistics(call: ServiceCall) -> None:
    """Reimport one debtor's statistics from the given date through yesterday."""
    entry_id: str = call.data[ATTR_CONFIG_ENTRY_ID]
    entry = _loaded_entry(call.hass, entry_id)
    await entry.runtime_data.coordinator.async_reimport_statistics(
        call.data[ATTR_START_DATE]
    )


def _loaded_entry(hass: HomeAssistant, entry_id: str) -> CoolblueConfigEntry:
    """Resolve *entry_id* to a loaded Coolblue Energy entry, or refuse to act.

    Reimport overwrites stored history, so an identifier that does not resolve
    must be an error naming it — never a fall-back to some other debtor.
    """
    entry = hass.config_entries.async_get_entry(entry_id)
    if (
        entry is None
        or entry.domain != DOMAIN
        or entry.state is not ConfigEntryState.LOADED
    ):
        raise ServiceValidationError(
            f"No loaded Coolblue Energy debtor has config entry id {entry_id}, "
            f"so nothing was reimported"
        )
    return entry

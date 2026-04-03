"""Coolblue Energy integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant

from .api_client import ApiClient
from .const import CONF_DEBTOR_ID, CONF_LOCATION_ID, DOMAIN, PLATFORMS
from .coordinator import CoolblueCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Coolblue Energy from a config entry."""
    client = ApiClient(entry.data[CONF_EMAIL], entry.data[CONF_PASSWORD])
    coordinator = CoolblueCoordinator(
        hass,
        client,
        entry.data[CONF_DEBTOR_ID],
        entry.data[CONF_LOCATION_ID],
    )

    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        "coordinator": coordinator,
        "client": client,
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        data = hass.data[DOMAIN].pop(entry.entry_id)
        await data["client"].close()
    return unloaded

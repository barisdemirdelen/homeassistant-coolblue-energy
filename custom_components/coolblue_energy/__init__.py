"""Coolblue Energy integration."""

from __future__ import annotations

import logging
from datetime import date

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv

from .api_client import ApiClient
from .const import (
    ATTR_START_DATE,
    CONF_DEBTOR_ID,
    CONF_LOCATION_ID,
    DOMAIN,
    PLATFORMS,
    SERVICE_REIMPORT_STATISTICS,
)
from .coordinator import CoolblueCoordinator

_LOGGER = logging.getLogger(__name__)

_REIMPORT_SCHEMA = vol.Schema({vol.Required(ATTR_START_DATE): cv.date})


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

    # Register the reimport service once for the whole domain.
    if not hass.services.has_service(DOMAIN, SERVICE_REIMPORT_STATISTICS):

        async def _handle_reimport(call: ServiceCall) -> None:
            start_date: date = call.data[ATTR_START_DATE]
            for entry_data in hass.data.get(DOMAIN, {}).values():
                coord: CoolblueCoordinator = entry_data["coordinator"]
                await coord.async_reimport_statistics(start_date)

        hass.services.async_register(
            DOMAIN,
            SERVICE_REIMPORT_STATISTICS,
            _handle_reimport,
            schema=_REIMPORT_SCHEMA,
        )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        data = hass.data[DOMAIN].pop(entry.entry_id)
        await data["client"].close()
        # Remove the service when the last config entry is unloaded.
        if not hass.data.get(DOMAIN):
            hass.services.async_remove(DOMAIN, SERVICE_REIMPORT_STATISTICS)
    return unloaded

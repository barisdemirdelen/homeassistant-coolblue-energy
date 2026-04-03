"""Config flow for Coolblue Energy."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp
import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD

from .api_client import ApiClient
from .const import CONF_DEBTOR_ID, CONF_LOCATION_ID, DEFAULT_NAME, DOMAIN

_LOGGER = logging.getLogger(__name__)

_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): str,
        vol.Required(CONF_PASSWORD): str,
    }
)


class CoolblueConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Coolblue Energy."""

    VERSION = 1

    # ── User step ─────────────────────────────────────────────────────────────

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial user step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            debtor_id, location_id, error = await self._try_connect(
                user_input[CONF_EMAIL], user_input[CONF_PASSWORD]
            )
            if error:
                errors["base"] = error
            else:
                await self.async_set_unique_id(debtor_id)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=DEFAULT_NAME,
                    data={
                        CONF_EMAIL: user_input[CONF_EMAIL],
                        CONF_PASSWORD: user_input[CONF_PASSWORD],
                        CONF_DEBTOR_ID: debtor_id,
                        CONF_LOCATION_ID: location_id,
                    },
                )

        return self.async_show_form(
            step_id="user",
            data_schema=_USER_SCHEMA,
            errors=errors,
        )

    # ── Reauth step ───────────────────────────────────────────────────────────

    async def async_step_reauth(self, entry_data: dict[str, Any]) -> ConfigFlowResult:
        """Trigger reauth when the session expires."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle credential re-entry."""
        errors: dict[str, str] = {}
        reauth_entry = self._get_reauth_entry()

        if user_input is not None:
            _, _, error = await self._try_connect(
                reauth_entry.data[CONF_EMAIL], user_input[CONF_PASSWORD]
            )
            if error:
                errors["base"] = error
            else:
                return self.async_update_reload_and_abort(
                    reauth_entry,
                    data_updates={CONF_PASSWORD: user_input[CONF_PASSWORD]},
                )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema({vol.Required(CONF_PASSWORD): str}),
            errors=errors,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _try_connect(email: str, password: str) -> tuple[str, str, str | None]:
        """
        Attempt to connect and return ``(debtor_id, location_id, error_key)``.

        ``error_key`` is ``None`` on success, or one of the keys in
        ``translations/en.json`` config.error on failure.
        """
        try:
            async with ApiClient(email, password) as client:
                debtor_id, location_id = await client.get_energy_ids()
            return debtor_id, location_id, None
        except aiohttp.ClientResponseError as exc:
            if exc.status in (401, 403):
                return "", "", "invalid_auth"
            _LOGGER.exception("HTTP error during Coolblue connect")
            return "", "", "cannot_connect"
        except RuntimeError as exc:
            if "credentials" in str(exc).lower():
                return "", "", "invalid_auth"
            _LOGGER.exception("Runtime error during Coolblue connect")
            return "", "", "cannot_connect"
        except Exception:
            _LOGGER.exception("Unexpected error during Coolblue connect")
            return "", "", "cannot_connect"

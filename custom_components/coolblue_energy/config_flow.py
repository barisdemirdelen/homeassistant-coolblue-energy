"""Config flow for Coolblue Energy."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .api_client import ApiClient
from .auth import CoolblueAuthError
from .const import CONF_DEBTOR_ID, CONF_LOCATION_ID, DEFAULT_NAME, DOMAIN

_LOGGER = logging.getLogger(__name__)


# A masked field offering the browser's stored Coolblue password. Shared by both
# steps: a selector is a validator, so one instance serves every schema.
_PASSWORD_FIELD = TextSelector(
    TextSelectorConfig(type=TextSelectorType.PASSWORD, autocomplete="current-password")
)

_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): TextSelector(
            TextSelectorConfig(type=TextSelectorType.EMAIL, autocomplete="email")
        ),
        vol.Required(CONF_PASSWORD): _PASSWORD_FIELD,
    }
)

_REAUTH_SCHEMA = vol.Schema({vol.Required(CONF_PASSWORD): _PASSWORD_FIELD})


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
                    title=f"{DEFAULT_NAME} (debtor {debtor_id})",
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
        """Trigger reauth when Coolblue rejects the stored credentials."""
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
            data_schema=_REAUTH_SCHEMA,
            errors=errors,
            description_placeholders={CONF_EMAIL: reauth_entry.data[CONF_EMAIL]},
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _try_connect(email: str, password: str) -> tuple[str, str, str | None]:
        """
        Attempt to connect and return ``(debtor_id, location_id, error_key)``.

        ``error_key`` is ``None`` on success, or one of the keys in
        ``translations/en.json`` config.error on failure. Only a rejection of the
        credentials themselves is reported as such; every other failure is a
        connection problem the user can retry.
        """
        try:
            async with ApiClient(email, password) as client:
                debtor_id, location_id = await client.get_energy_ids()
            return debtor_id, location_id, None
        except CoolblueAuthError:
            _LOGGER.debug("Coolblue rejected the credentials for %s", email)
            return "", "", "invalid_auth"
        except Exception:
            _LOGGER.exception("Could not connect to Coolblue")
            return "", "", "cannot_connect"

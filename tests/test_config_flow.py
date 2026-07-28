"""Config flow, driven as a user drives it: through Home Assistant's flow manager.

Every test here goes through ``hass.config_entries.flow``, so what is asserted is
what the user sees — the step, its fields, its errors, and the entry that comes
out — rather than the credential check in isolation. The re-authentication path
went unnoticed while it was broken precisely because nothing entered a step.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from homeassistant.config_entries import SOURCE_USER
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers.selector import TextSelector, TextSelectorType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.coolblue_energy.auth import CoolblueAuthError
from custom_components.coolblue_energy.const import (
    CONF_DEBTOR_ID,
    CONF_LOCATION_ID,
    DOMAIN,
)

from .conftest import DEBTOR_ID, ENTRY_DATA, LOCATION_ID

# The credentials the shared entry already holds, so a reauth test can tell an
# unchanged password from a replaced one.
_CREDENTIALS = {
    CONF_EMAIL: ENTRY_DATA[CONF_EMAIL],
    CONF_PASSWORD: ENTRY_DATA[CONF_PASSWORD],
}

_EN_JSON = (
    Path(__file__).parent.parent
    / "custom_components/coolblue_energy/translations/en.json"
)


def _patch_client(client: AsyncMock):
    """Make ``async with ApiClient(...)`` in the flow yield *client*."""
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=client)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    return patch("custom_components.coolblue_energy.config_flow.ApiClient", factory)


def _no_setup():
    """Keep the flow's entry from being loaded — the flow is what is under test."""
    return patch(
        "custom_components.coolblue_energy.async_setup_entry", return_value=True
    )


def _fields(result: Mapping[str, Any]) -> dict[str, Any]:
    """The form's fields, keyed by name."""
    return {str(key): value for key, value in result["data_schema"].schema.items()}


@pytest.fixture
def flow_client() -> AsyncMock:
    """An API client that resolves the credentials to this account's ids."""
    client = AsyncMock()
    client.get_energy_ids.return_value = (DEBTOR_ID, LOCATION_ID)
    return client


# ── The initial step ──────────────────────────────────────────────────────────


async def test_user_step_asks_for_an_email_and_a_masked_password(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
) -> None:
    """Credentials are typed, not bare text: e-mail autofills, password hides."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    fields = _fields(result)
    assert isinstance(fields[CONF_EMAIL], TextSelector)
    assert fields[CONF_EMAIL].config["type"] == TextSelectorType.EMAIL
    assert isinstance(fields[CONF_PASSWORD], TextSelector)
    assert fields[CONF_PASSWORD].config["type"] == TextSelectorType.PASSWORD


async def test_valid_credentials_create_an_entry_for_the_debtor(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    flow_client: AsyncMock,
) -> None:
    """The entry carries everything a later poll needs, keyed by its debtor.

    The unique id is what refuses a second entry for the same debtor, and the
    title is all the reimport action's picker shows, so both have to name it.
    """
    started = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )

    with _patch_client(flow_client), _no_setup():
        result = await hass.config_entries.flow.async_configure(
            started["flow_id"], dict(_CREDENTIALS)
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert DEBTOR_ID in result["title"]
    assert result["data"] == {
        **_CREDENTIALS,
        CONF_DEBTOR_ID: DEBTOR_ID,
        CONF_LOCATION_ID: LOCATION_ID,
    }
    assert result["result"].unique_id == DEBTOR_ID


async def test_rejected_credentials_are_reported_as_such_and_can_be_retyped(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    flow_client: AsyncMock,
) -> None:
    """A wrong password says so, and the form comes back ready for the right one."""
    started = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )

    flow_client.get_energy_ids.side_effect = CoolblueAuthError("refused")
    with _patch_client(flow_client):
        rejected = await hass.config_entries.flow.async_configure(
            started["flow_id"], {**_CREDENTIALS, CONF_PASSWORD: "wrong"}
        )

    assert rejected["type"] is FlowResultType.FORM
    assert rejected["step_id"] == "user"
    assert rejected["errors"] == {"base": "invalid_auth"}

    flow_client.get_energy_ids.side_effect = None
    with _patch_client(flow_client), _no_setup():
        retried = await hass.config_entries.flow.async_configure(
            started["flow_id"], dict(_CREDENTIALS)
        )
        await hass.async_block_till_done()

    assert retried["type"] is FlowResultType.CREATE_ENTRY


async def test_unreachable_coolblue_is_reported_as_a_connection_problem(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    flow_client: AsyncMock,
) -> None:
    """An outage is not a wrong password, so it must not say the password is wrong."""
    started = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )

    flow_client.get_energy_ids.side_effect = aiohttp.ClientError("unreachable")
    with _patch_client(flow_client):
        result = await hass.config_entries.flow.async_configure(
            started["flow_id"], dict(_CREDENTIALS)
        )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}


async def test_second_entry_for_the_same_debtor_is_refused(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    flow_client: AsyncMock,
) -> None:
    """Two entries for one debtor would write every statistic twice."""
    config_entry.add_to_hass(hass)
    started = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )

    with _patch_client(flow_client), _no_setup():
        result = await hass.config_entries.flow.async_configure(
            started["flow_id"], dict(_CREDENTIALS)
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


# ── Re-authentication ─────────────────────────────────────────────────────────


async def test_reauth_asks_only_for_a_masked_password(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
) -> None:
    """The e-mail is already known; what changed is the password."""
    config_entry.add_to_hass(hass)

    result = await config_entry.start_reauth_flow(hass)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reauth_confirm"
    fields = _fields(result)
    assert set(fields) == {CONF_PASSWORD}
    assert fields[CONF_PASSWORD].config["type"] == TextSelectorType.PASSWORD
    placeholders = result["description_placeholders"] or {}
    assert placeholders[CONF_EMAIL] == _CREDENTIALS[CONF_EMAIL]


async def test_every_credential_field_says_it_is_a_coolblue_account_field(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
) -> None:
    """A Coolblue login is not obviously the thing being asked for, so each field says so."""
    config_entry.add_to_hass(hass)
    steps = json.loads(_EN_JSON.read_text(encoding="utf-8"))["config"]["step"]

    user_step = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    reauth_step = await config_entry.start_reauth_flow(hass)

    for step in (user_step, reauth_step):
        described = steps[step["step_id"]]["data_description"]
        for field in _fields(step):
            assert "coolblue" in described[field].lower()


async def test_reauth_stores_a_new_password_and_rejects_a_wrong_one(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    flow_client: AsyncMock,
) -> None:
    """The whole sequence: prompted, refused once, then accepted and stored."""
    config_entry.add_to_hass(hass)
    started = await config_entry.start_reauth_flow(hass)

    flow_client.get_energy_ids.side_effect = CoolblueAuthError("refused")
    with _patch_client(flow_client):
        rejected = await hass.config_entries.flow.async_configure(
            started["flow_id"], {CONF_PASSWORD: "still-wrong"}
        )

    assert rejected["type"] is FlowResultType.FORM
    assert rejected["step_id"] == "reauth_confirm"
    assert rejected["errors"] == {"base": "invalid_auth"}
    assert config_entry.data[CONF_PASSWORD] == _CREDENTIALS[CONF_PASSWORD]

    flow_client.get_energy_ids.side_effect = None
    with _patch_client(flow_client), _no_setup():
        accepted = await hass.config_entries.flow.async_configure(
            started["flow_id"], {CONF_PASSWORD: "new-password"}
        )
        await hass.async_block_till_done()

    assert accepted["type"] is FlowResultType.ABORT
    assert accepted["reason"] == "reauth_successful"
    assert config_entry.data[CONF_PASSWORD] == "new-password"
    assert config_entry.data[CONF_EMAIL] == _CREDENTIALS[CONF_EMAIL]


async def test_reauth_checks_the_stored_email_with_the_new_password(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    flow_client: AsyncMock,
) -> None:
    """The form asks for one field, so the check has to supply the other itself."""
    config_entry.add_to_hass(hass)
    started = await config_entry.start_reauth_flow(hass)

    with _patch_client(flow_client) as factory, _no_setup():
        await hass.config_entries.flow.async_configure(
            started["flow_id"], {CONF_PASSWORD: "new-password"}
        )
        await hass.async_block_till_done()

    factory.assert_called_once_with(_CREDENTIALS[CONF_EMAIL], "new-password")

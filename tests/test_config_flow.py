"""
test_config_flow.py

Tests for CoolblueConfigFlow._try_connect — the static method that validates
credentials against the live API — and for the entry the user step creates.
All network calls are mocked.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from homeassistant.config_entries import SOURCE_USER
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType

from custom_components.coolblue_energy.config_flow import CoolblueConfigFlow
from custom_components.coolblue_energy.const import DOMAIN

from .conftest import DEBTOR_ID, LOCATION_ID

_PATCH = "custom_components.coolblue_energy.config_flow.ApiClient"


def _mock_client(return_value=None, side_effect=None) -> tuple:
    """Return (mock_cls, mock_instance) for patching ApiClient."""
    mock_instance = AsyncMock()
    if side_effect:
        mock_instance.get_energy_ids.side_effect = side_effect
    else:
        mock_instance.get_energy_ids.return_value = return_value or (
            "00844083",
            "uuid-loc-123",
        )
    mock_cls = MagicMock()
    mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
    mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    return mock_cls, mock_instance


class TestTryConnect:
    """Unit tests for CoolblueConfigFlow._try_connect."""

    async def test_success_returns_ids_and_no_error(self):
        """Valid credentials → (debtor_id, location_id, None)."""
        mock_cls, _ = _mock_client(return_value=("00844083", "uuid-loc-123"))
        with patch(_PATCH, mock_cls):
            debtor, loc, err = await CoolblueConfigFlow._try_connect(
                "user@test.com", "correct-password"
            )
        assert debtor == "00844083"
        assert loc == "uuid-loc-123"
        assert err is None

    async def test_runtime_error_with_credentials_gives_invalid_auth(self):
        """RuntimeError mentioning 'credentials' → 'invalid_auth'."""
        mock_cls, _ = _mock_client(
            side_effect=RuntimeError("OIDC round failed – check credentials.")
        )
        with patch(_PATCH, mock_cls):
            _, _, err = await CoolblueConfigFlow._try_connect(
                "user@test.com", "wrong-password"
            )
        assert err == "invalid_auth"

    async def test_http_401_gives_invalid_auth(self):
        """HTTP 401 from the portal → 'invalid_auth'."""
        exc = aiohttp.ClientResponseError(MagicMock(), (), status=401)
        mock_cls, _ = _mock_client(side_effect=exc)
        with patch(_PATCH, mock_cls):
            _, _, err = await CoolblueConfigFlow._try_connect(
                "user@test.com", "wrong-password"
            )
        assert err == "invalid_auth"

    async def test_http_403_gives_invalid_auth(self):
        """HTTP 403 → 'invalid_auth'."""
        exc = aiohttp.ClientResponseError(MagicMock(), (), status=403)
        mock_cls, _ = _mock_client(side_effect=exc)
        with patch(_PATCH, mock_cls):
            _, _, err = await CoolblueConfigFlow._try_connect(
                "user@test.com", "wrong-password"
            )
        assert err == "invalid_auth"

    async def test_http_503_gives_cannot_connect(self):
        """HTTP 503 (server down, not an auth issue) → 'cannot_connect'."""
        exc = aiohttp.ClientResponseError(MagicMock(), (), status=503)
        mock_cls, _ = _mock_client(side_effect=exc)
        with patch(_PATCH, mock_cls):
            _, _, err = await CoolblueConfigFlow._try_connect(
                "user@test.com", "any-password"
            )
        assert err == "cannot_connect"

    async def test_generic_exception_gives_cannot_connect(self):
        """Connection refused / timeout → 'cannot_connect'."""
        mock_cls, _ = _mock_client(side_effect=ConnectionRefusedError("refused"))
        with patch(_PATCH, mock_cls):
            _, _, err = await CoolblueConfigFlow._try_connect(
                "user@test.com", "any-password"
            )
        assert err == "cannot_connect"

    async def test_runtime_error_without_credentials_gives_cannot_connect(self):
        """RuntimeError NOT about credentials → 'cannot_connect'."""
        mock_cls, _ = _mock_client(side_effect=RuntimeError("Page structure changed"))
        with patch(_PATCH, mock_cls):
            _, _, err = await CoolblueConfigFlow._try_connect(
                "user@test.com", "any-password"
            )
        assert err == "cannot_connect"


async def test_user_step_titles_the_entry_with_its_debtor(
    recorder_mock: None,
    enable_custom_integrations: None,
    hass: HomeAssistant,
) -> None:
    """The title has to tell two debtors apart wherever entries are listed.

    The reimport action targets a config entry through a picker that shows
    nothing but the entry title, and reimport overwrites stored history — so a
    title that is the same for every debtor is how the wrong one gets picked.
    """
    mock_cls, _ = _mock_client(return_value=(DEBTOR_ID, LOCATION_ID))

    with (
        patch(_PATCH, mock_cls),
        patch("custom_components.coolblue_energy.async_setup_entry", return_value=True),
    ):
        started = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            started["flow_id"],
            {CONF_EMAIL: "user@example.com", CONF_PASSWORD: "hunter2"},
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert DEBTOR_ID in result["title"]

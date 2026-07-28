"""
test_api_client.py

Tests for robustness features in ApiClient and _parse_rsc_response.

Focus: make the API client resilient to Coolblue's frequent changes:
  1. RSC response format may change (line prefix versioning)
  2. Action IDs need retry on transient failures
  3. Energy ID extraction needs fallback strategies
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from custom_components.coolblue_energy.api_client import ApiClient, _parse_rsc_response
from custom_components.coolblue_energy.auth import CoolblueAuthError

# ── HTTP response doubles ─────────────────────────────────────────────────────

_CSRF_PAGE = '<html><body><form><input name="csrf" value="tok"></form></body></html>'
_ACCOUNTS_URL = "https://accounts.coolblue.nl/connect/authorize"
_SESSION = "custom_components.coolblue_energy.auth.aiohttp.ClientSession"


def _page(
    html: str = "", *, status: int = 200, url: str = _ACCOUNTS_URL, location: str = ""
) -> MagicMock:
    """One HTTP response, usable as the async context manager aiohttp returns."""
    resp = MagicMock(status=status, url=url)
    resp.headers = {"Location": location} if location else {}
    resp.text = AsyncMock(return_value=html)
    if status >= 400:
        resp.raise_for_status = MagicMock(
            side_effect=aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=status
            )
        )
    else:
        resp.raise_for_status = MagicMock()
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=None)
    return resp


def _login_session(*, gets: list, posts: list) -> MagicMock:
    """A stand-in ClientSession that replays *gets* and *posts* in order."""
    session = MagicMock(closed=False)
    session.get = MagicMock(side_effect=gets)
    session.post = MagicMock(side_effect=posts)
    session.close = AsyncMock()
    session.cookie_jar.filter_cookies = MagicMock(return_value={})
    return session


# ── RSC Parsing Robustness ────────────────────────────────────────────────────


class TestParseRscResponse:
    """_parse_rsc_response must tolerate version changes in line prefix."""

    def test_parses_standard_two_line_format(self):
        """Standard format: '0:metadata\n1:payload'"""
        text = '0:{"a":"$@1"}\n1:[{"id":1}]'
        result = _parse_rsc_response(text)
        assert result == [{"id": 1}]

    def test_parses_payload_without_line_prefix(self):
        """Some Next.js versions omit line prefixes entirely."""
        text = '{"a":"$@1"}\n[{"id":2}]'
        result = _parse_rsc_response(text)
        assert result == [{"id": 2}]

    def test_parses_numeric_line_prefix(self):
        """Prefix may be 'N:' where N changes (e.g. Next.js bump)."""
        text = '0:{"a":"$@1"}\n5:[{"id":3}]'
        result = _parse_rsc_response(text)
        assert result == [{"id": 3}]

    def test_handles_blank_lines_between_payloads(self):
        """Blank lines in response should not break parsing."""
        text = '0:{}\n\n[{"id":4}]\n'
        result = _parse_rsc_response(text)
        assert result == [{"id": 4}]

    def test_handles_object_payload_not_just_list(self):
        """Payload may be a dict, not just a list."""
        text = '0:{}\n1:{"data":{"value":42}}'
        result = _parse_rsc_response(text)
        assert result == {"data": {"value": 42}}

    def test_raises_on_no_valid_json(self):
        """Pure garbage must still raise."""
        text = "garbage\nmore garbage"
        with pytest.raises(ValueError, match="Could not find payload"):
            _parse_rsc_response(text)


# ── Retry Logic ───────────────────────────────────────────────────────────────


class TestRetryOnTransientFailure:
    @pytest.mark.asyncio
    async def test_retries_on_server_error_then_succeeds(self):
        """502/503 should trigger retries; eventual success returns result."""
        client = ApiClient("test@test.com", "pass")
        call_count = [0]

        def side_effect(*_args, **_kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                raise aiohttp.ClientResponseError(
                    request_info=MagicMock(),
                    history=(),
                    status=(502 if call_count[0] == 1 else 503),
                )
            return '{"result":"ok"}'

        with patch.object(client, "_next_action_post", side_effect=side_effect):
            result = await client._retry_with_backoff(
                fn_name="getInsights",
                operation=lambda: client._next_action_post("action", []),
            )

        assert call_count[0] == 3
        assert "ok" in result

    @pytest.mark.asyncio
    async def test_exhausted_retries_raise_last_error(self):
        """When all retries fail, the final exception is raised."""
        client = ApiClient("test@test.com", "pass")
        call_count = [0]

        def always_fail(*_args, **_kwargs):
            call_count[0] += 1
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=502
            )

        with (
            patch.object(client, "_next_action_post", side_effect=always_fail),
            pytest.raises(aiohttp.ClientResponseError),
        ):
            await client._retry_with_backoff(
                fn_name="getInsights",
                operation=lambda: client._next_action_post("a", []),
            )

        # default 3 attempts (1 initial + 2 retries)
        assert call_count[0] == 3

    @pytest.mark.asyncio
    async def test_no_retry_on_client_error(self):
        """4xx errors should NOT be retried — they're permanent."""
        client = ApiClient("test@test.com", "pass")
        call_count = [0]

        def always_403(*_args, **_kwargs):
            call_count[0] += 1
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=403
            )

        with (
            patch.object(client, "_next_action_post", side_effect=always_403),
            pytest.raises(aiohttp.ClientResponseError),
        ):
            await client._retry_with_backoff(
                fn_name="getInsights",
                operation=lambda: client._next_action_post("a", []),
            )

        assert call_count[0] == 1  # no retry for 4xx

    @pytest.mark.asyncio
    async def test_retries_on_timeout(self):
        """asyncio.TimeoutError triggers retries like server errors."""
        client = ApiClient("test@test.com", "pass")
        call_count = [0]

        def timeout_then_work(*_args, **_kwargs):
            call_count[0] += 1
            if call_count[0] < 2:
                raise TimeoutError()
            return '{"recovered":true}'

        with patch.object(client, "_next_action_post", side_effect=timeout_then_work):
            result = await client._retry_with_backoff(
                fn_name="getInsights",
                operation=lambda: client._next_action_post("a", []),
            )

        assert call_count[0] == 2
        assert "recovered" in result


# ── Energy ID Extraction Fallbacks ───────────────────────────────────────────


class TestEnergyIdExtractionFallback:
    @pytest.mark.asyncio
    async def test_falls_back_to_next_data_script(self):
        """When self.__next_f.push pattern is missing, parse __NEXT_DATA__."""
        client = ApiClient("test@test.com", "pass")

        html = """<html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"debtorNumber":"12345678","locationId":"deadbeef-0000-0000-0000-000000000000"}}}
        </script></body></html>"""

        # session.get() is sync and returns an async context manager (not a coroutine)
        mock_session = MagicMock(closed=False)
        mock_session.get = MagicMock(return_value=_page(html))
        client._get_session = AsyncMock(return_value=mock_session)

        debtor, location = await client.get_energy_ids()
        assert debtor == "12345678"
        assert location.startswith("deadbeef")


# ── Credential Rejection ──────────────────────────────────────────────────────


class TestCredentialRejection:
    """A rejected password is its own exception type, not a generic failure."""

    async def test_password_not_accepted_raises_auth_error(self):
        """The portal answers the password POST with the form again, not a redirect."""
        session = _login_session(
            gets=[_page(_CSRF_PAGE)],
            posts=[_page(_CSRF_PAGE), _page(_CSRF_PAGE)],
        )
        with patch(_SESSION, return_value=session), pytest.raises(CoolblueAuthError):
            async with ApiClient("user@example.com", "wrong") as client:
                await client.get_energy_ids()

    async def test_callback_landing_back_on_accounts_raises_auth_error(self):
        """The OIDC callback bounces back to the accounts page: not logged in."""
        session = _login_session(
            gets=[_page(_CSRF_PAGE), _page("", url=f"{_ACCOUNTS_URL}/login")],
            posts=[
                _page(_CSRF_PAGE),
                _page(status=302, location="/connect/callback?code=x"),
            ],
        )
        with patch(_SESSION, return_value=session), pytest.raises(CoolblueAuthError):
            async with ApiClient("user@example.com", "wrong") as client:
                await client.get_energy_ids()

    async def test_server_error_during_login_is_not_an_auth_error(self):
        """A 500 while logging in is a broken portal, not a wrong password."""
        session = _login_session(
            gets=[_page(_CSRF_PAGE)],
            posts=[_page(_CSRF_PAGE), _page(status=500)],
        )
        with patch(_SESSION, return_value=session):
            async with ApiClient("user@example.com", "hunter2") as client:
                with pytest.raises(aiohttp.ClientResponseError) as caught:
                    await client.get_energy_ids()
        assert not isinstance(caught.value, CoolblueAuthError)

    async def test_cloudfront_block_is_not_an_auth_error(self):
        """A WAF 403 on the password POST is a connection problem, not credentials."""
        session = _login_session(
            gets=[_page(_CSRF_PAGE)],
            posts=[_page(_CSRF_PAGE), _page(status=403)],
        )
        with patch(_SESSION, return_value=session):
            async with ApiClient("user@example.com", "hunter2") as client:
                with pytest.raises(RuntimeError) as caught:
                    await client.get_energy_ids()
        assert not isinstance(caught.value, CoolblueAuthError)

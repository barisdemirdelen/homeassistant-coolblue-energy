"""
test_api_client.py

Tests for robustness features in ApiClient.

Focus: make the API client resilient to Coolblue's frequent changes:
  1. Hourly insights requests match the portal's contract
  2. Transient portal failures are retried, permanent ones are not
  3. Energy ID extraction needs fallback strategies
"""

from __future__ import annotations

import json
from datetime import date
from typing import Literal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from custom_components.coolblue_energy.api_client import ApiClient
from custom_components.coolblue_energy.auth import CoolblueAuthError
from custom_components.coolblue_energy.model import GetMeterReadingsRequest

from .conftest import DEBTOR_ID, LOCATION_ID

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


def _data_session(*responses: MagicMock | Exception) -> MagicMock:
    """A stand-in logged-in ClientSession whose GETs replay *responses* in order."""
    session = MagicMock(closed=False)
    session.get = MagicMock(side_effect=list(responses))
    return session


def _client_on(session: MagicMock) -> ApiClient:
    """An ApiClient that is already logged in on *session*."""
    client = ApiClient("test@test.com", "pass")
    client._get_session = AsyncMock(return_value=session)
    return client


# One hour as the portal's /api/insights returns it (trimmed live response).
_INSIGHTS_BODY = json.dumps(
    [
        {
            "timestamp": "2026-09-24T00:00:00.000Z",
            "electricity": {
                "usage": {"peak": 0, "offPeak": 0.47, "single": 0.47, "total": 0.47},
                "cost": {"amount": 0.15},
            },
            "gas": {"usage": 0, "cost": {"amount": 0}},
            "dynamicPrice": 0.34,
            "smartDevices": None,
        }
    ]
)


def _mislabelled_day(day: date, rows: int) -> str:
    """A day of rows as /api/insights (v1) sends them: one per wall-clock hour,
    in order, but labelled ``max(i - 1, 0)`` (observed live, 2026-09)."""
    return json.dumps(
        [
            {"timestamp": f"{day}T{max(i - 1, 0):02d}:00:00.000Z", "dynamicPrice": i}
            for i in range(rows)
        ]
    )


def _insights_request(
    energy_type: Literal["electricity", "gas", "costs"] = "electricity",
    for_date: date = date(2026, 9, 24),
) -> GetMeterReadingsRequest:
    return GetMeterReadingsRequest(
        customer_id=DEBTOR_ID,
        connection_uuid=LOCATION_ID,
        energy_type=energy_type,
        for_date=for_date,
    )


# ── Hourly Insights ───────────────────────────────────────────────────────────


class TestHourlyEnergy:
    async def test_requests_hourly_insights_for_the_day_and_parses_entries(self):
        session = _data_session(_page(_INSIGHTS_BODY))
        client = _client_on(session)

        entries = await client.get_hourly_energy(_insights_request("gas"))

        (url,), kwargs = session.get.call_args
        assert url == "https://www.coolblue.nl/api/insights"
        assert kwargs["params"] == {
            "granularity": "HOUR",
            # Midnight Amsterdam (CEST, UTC+2) on the requested day, in UTC.
            "from": "2026-09-23T22:00:00.000Z",
            "year": "2026",
            "month": "9",
            "day": "24",
            "locationId": LOCATION_ID,
            "debtorNumber": DEBTOR_ID,
            "commodity": "gas",
            "hasInsightV2": "false",
        }
        assert [(e.name, e.dynamic_price) for e in entries] == [("00:00", 0.34)]
        assert entries[0].electricity.usage.total == 0.47

    @pytest.mark.parametrize(
        "day",
        [
            date(2026, 9, 24),
            # Autumn fall-back: the portal folds the doubled 02:00 into one row.
            date(2025, 10, 26),
        ],
    )
    async def test_rows_are_labelled_by_position_not_by_the_api_timestamp(
        self, day: date
    ):
        session = _data_session(_page(_mislabelled_day(day, 24)))

        entries = await _client_on(session).get_hourly_energy(
            _insights_request(for_date=day)
        )

        assert [(e.name, e.dynamic_price) for e in entries] == [
            (f"{h:02d}:00", h) for h in range(24)
        ]

    async def test_spring_forward_day_skips_the_missing_hour(self):
        """2026-03-29 has 23 rows: 02:00 does not exist in Amsterdam."""
        day = date(2026, 3, 29)
        session = _data_session(_page(_mislabelled_day(day, 23)))

        entries = await _client_on(session).get_hourly_energy(
            _insights_request(for_date=day)
        )

        assert [e.name for e in entries] == [
            "00:00", "01:00", "03:00", "04:00", "05:00", "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00",
            "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00",
        ]  # fmt: skip

    async def test_more_rows_than_hours_in_the_day_is_a_value_error(self):
        """25 rows cannot be placed on a day with 24 wall-clock hours."""
        day = date(2025, 10, 26)
        session = _data_session(_page(_mislabelled_day(day, 25)))

        with pytest.raises(ValueError, match="at most 24"):
            await _client_on(session).get_hourly_energy(_insights_request(for_date=day))


# ── Retry Logic ───────────────────────────────────────────────────────────────


class TestRetryOnTransientFailure:
    async def test_retries_server_errors_then_returns_data(self):
        session = _data_session(
            _page(status=502), _page(status=503), _page(_INSIGHTS_BODY)
        )

        entries = await _client_on(session).get_hourly_energy(_insights_request())

        assert session.get.call_count == 3
        assert [e.name for e in entries] == ["00:00"]

    async def test_exhausted_retries_raise_last_error(self):
        session = _data_session(_page(status=502), _page(status=502), _page(status=504))

        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await _client_on(session).get_hourly_energy(_insights_request())

        # default 3 attempts (1 initial + 2 retries)
        assert session.get.call_count == 3
        assert caught.value.status == 504

    async def test_no_retry_on_client_error(self):
        session = _data_session(_page(status=403), _page(_INSIGHTS_BODY))

        with pytest.raises(aiohttp.ClientResponseError):
            await _client_on(session).get_hourly_energy(_insights_request())

        assert session.get.call_count == 1

    async def test_retries_on_timeout(self):
        session = _data_session(TimeoutError(), _page(_INSIGHTS_BODY))

        entries = await _client_on(session).get_hourly_energy(_insights_request())

        assert session.get.call_count == 2
        assert [e.name for e in entries] == ["00:00"]

    async def test_non_json_response_is_a_value_error_without_retry(self):
        """An HTML page instead of JSON (e.g. a login redirect) fails fast."""
        session = _data_session(_page("<html>login</html>"), _page(_INSIGHTS_BODY))

        with pytest.raises(ValueError):
            await _client_on(session).get_hourly_energy(_insights_request())

        assert session.get.call_count == 1


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

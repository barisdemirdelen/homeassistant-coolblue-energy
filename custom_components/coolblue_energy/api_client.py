"""
api_client.py

Energy API calls for the Coolblue Energy portal.

Usage::

    async with ApiClient("you@example.com", "secret") as client:
        debtor_id, location_id = await client.get_energy_ids()
        entries = await client.get_hourly_energy(GetMeterReadingsRequest(...))

Hourly data comes from the portal's own JSON endpoint (``GET /api/insights``),
authenticated by the session cookies :class:`~auth.AuthService` obtains.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Self

import aiohttp
from pydantic import TypeAdapter

from .auth import AuthService
from .model import GetMeterReadingsRequest, MeterReadingEntry

logger = logging.getLogger(__name__)

_MeterReadingList = TypeAdapter(list[MeterReadingEntry])

# ── Module-level constants ────────────────────────────────────────────────────

ENERGY_URL = "https://www.coolblue.nl/nl/mijn-coolblue-account/energie/energieverbruik"
INSIGHTS_URL = "https://www.coolblue.nl/api/insights"

#: Closed set of exceptions the public ``ApiClient`` methods raise on failure:
#: transport errors and timeouts from aiohttp, plus the ``RuntimeError`` /
#: ``ValueError`` that ``_retry_with_backoff`` re-raises for deterministic
#: parse/contract failures. Callers catch this instead of a blind ``Exception``.
API_ERRORS: tuple[type[Exception], ...] = (
    aiohttp.ClientError,
    TimeoutError,
    RuntimeError,
    ValueError,
)


class ApiClient:
    """
    Async Coolblue Energy API client.

    Owns an :class:`~auth.AuthService` and lazily authenticates on first use.
    Use as an async context manager to ensure the session is properly closed.

    :param email:    Coolblue account e-mail address.
    :param password: Coolblue account password.
    """

    def __init__(self, email: str, password: str) -> None:
        self._auth = AuthService(email, password, ENERGY_URL)

    async def _get_session(self) -> aiohttp.ClientSession:
        return await self._auth.get_session()

    @staticmethod
    async def _retry_with_backoff(fn_name: str, operation, max_retries: int = 2):
        """Execute *operation* with retries on transient failures.

        Retries on: server errors (5xx), connection timeouts.
        Does NOT retry on client errors (4xx) - those are permanent.

        Backoff starts at 0.3s and doubles each attempt.
        """
        last_exc: Exception | None = None
        delay = 0.3
        for attempt in range(1 + max_retries):
            try:
                return await operation()
            except aiohttp.ClientResponseError as exc:
                if 400 <= exc.status < 500:
                    logger.debug(
                        "Not retrying %s on client error %d", fn_name, exc.status
                    )
                    raise
                last_exc = exc
                logger.warning(
                    "%s attempt %d failed (HTTP %d), retrying in %.1fs ...",
                    fn_name,
                    attempt + 1,
                    exc.status,
                    delay,
                )
            except TimeoutError as exc:
                last_exc = exc
                logger.warning(
                    "%s attempt %d timed out, retrying in %.1fs ...",
                    fn_name,
                    attempt + 1,
                    delay,
                )
            except RuntimeError, ValueError:
                # Deterministic failures (bad data, field renames, etc.) are not retried.
                raise
            await asyncio.sleep(delay)
            delay *= 2

        assert last_exc is not None
        raise last_exc

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _extract_from_next_data(html: str) -> tuple[str, str] | None:
        """Try to extract debtor/location from __NEXT_DATA__ script."""
        nx_match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not nx_match:
            return None

        try:
            data = json.loads(nx_match.group(1))
        except json.JSONDecodeError, ValueError:
            return None

        def _dig(d, key):
            """Recursively search for *key* in nested dicts/lists."""
            match d:
                case dict():
                    if key in d:
                        return d[key]
                    for v in d.values():
                        found = _dig(v, key)
                        if found is not None:
                            return found
                case list():
                    for item in d:
                        found = _dig(item, key)
                        if found is not None:
                            return found
            return None

        debtor = _dig(data, "debtorNumber") or _dig(data, "id")
        location = _dig(data, "locationId") or _dig(data, "uuid")
        if debtor and location:
            return str(debtor), str(location)
        return None

    # ── Public API ────────────────────────────────────────────────────────────

    async def get_energy_ids(self) -> tuple[str, str]:
        """
        Fetch the energy page and extract ``debtorNumber`` and ``locationId``
        from the embedded Next.js RSC payload.

        Returns ``(debtor_number, location_uuid)``.
        """
        session = await self._get_session()
        async with session.get(ENERGY_URL) as r:
            r.raise_for_status()
            html = await r.text()

        chunks = re.findall(
            r'self\.__next_f\.push\(\[1,\s*"((?:[^"\\]|\\.)*)"]\)', html
        )
        full_rsc = "\n".join(json.loads(f'"{c}"') for c in chunks)

        debtor = re.search(r'"debtorNumber"\s*:\s*"(\d+)"', full_rsc)
        location = re.search(
            r'"locationId"\s*:\s*"([0-9a-f]{8}-[0-9a-f-]{27})"', full_rsc
        )

        if debtor and location:
            return debtor.group(1), location.group(1)

        # Strategy 2: __NEXT_DATA__ script tag (SSR fallback)
        fallback_result = self._extract_from_next_data(html)
        if fallback_result:
            return fallback_result

        raise RuntimeError(
            "Could not find debtorNumber / locationId in energy page.\n"
            f"Tried RSC chunks ({len(chunks)} found), __NEXT_DATA__ script.\n"
            f"Page length: {len(html)}"
        )

    async def get_hourly_energy(
        self, request: GetMeterReadingsRequest
    ) -> list[MeterReadingEntry]:
        """
        Fetch hourly energy data for the date specified in *request*.

        Calls ``GET /api/insights`` and returns the JSON response parsed into
        typed :class:`~model.MeterReadingEntry` objects.
        Retries on transient failures (5xx, timeouts).

        The portal returns one row per wall-clock hour, in order, but labels
        row *i* as hour ``max(i - 1, 0)``. Row position is therefore the only
        reliable hour, and each entry's ``timestamp`` is rewritten from it.
        """

        async def fetch() -> str:
            session = await self._get_session()
            async with session.get(
                INSIGHTS_URL,
                params=request.to_query_params(),
                headers={"Accept": "application/json"},
            ) as r:
                r.raise_for_status()
                return await r.text()

        raw = await self._retry_with_backoff(fn_name="insights", operation=fetch)
        entries = _MeterReadingList.validate_json(raw)
        hours = request.hour_timestamps()
        if len(entries) > len(hours):
            raise ValueError(
                f"Expected at most {len(hours)} hourly rows for "
                f"{request.for_date}, got {len(entries)}"
            )
        return [
            entry.model_copy(update={"timestamp": ts})
            for entry, ts in zip(entries, hours, strict=False)
        ]

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        await self._auth.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

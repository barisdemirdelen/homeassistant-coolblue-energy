"""
api_client.py

Energy API calls for the Coolblue Energy portal.

Usage::

    async with ApiClient("you@example.com", "secret") as client:
        debtor_id, location_id = await client.get_energy_ids()
        entries = await client.get_hourly_energy(GetMeterReadingsRequest(...))

Action IDs:
  Next.js server-action hashes change on every deployment.  They are
  discovered dynamically from the public JS chunks on first use and cached
  per-instance in ``_action_cache``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re

import aiohttp
from pydantic import TypeAdapter

from .auth import AuthService
from .model import GetMeterReadingsRequest, MeterReadingEntry

logger = logging.getLogger(__name__)

_MeterReadingList = TypeAdapter(list[MeterReadingEntry])

# ── Module-level constants ────────────────────────────────────────────────────

ENERGY_URL = "https://www.coolblue.nl/en/my-coolblue-account/energy/energy-usage"

# URL-encoded next-router-state-tree for the energy-usage page
# (derived from observed browser traffic; update if the route structure changes)
_NEXT_ROUTER_STATE_TREE = (
    "%5B%22%22%2C%7B%22children%22%3A%5B%22(pages)%22%2C%7B%22children%22%3A%5B"
    "%22mijn-coolblue-account%22%2C%7B%22children%22%3A%5B%22energie%22%2C%7B%22"
    "children%22%3A%5B%22(energyContracts)%22%2C%7B%22children%22%3A%5B%22energi"
    "everbruik%22%2C%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%2Cnull%2Cnull"
    "%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%"
    "2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%2Ctrue%5D"
)

# Matches: (0, X.createServerReference)('ACTION_ID', ..., 'functionName')
# Works on both prettified and minified JS.
_SERVER_ACTION_RE = re.compile(
    r"createServerReference\s*\)\s*\(\s*['\"]([0-9a-f]{30,})['\"]"
    r"[^)]*?"
    r"['\"]([A-Za-z_]\w*)['\"]"
    r"[^)]*?\)"
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_rsc_response(text: str):
    """
    Parse a Next.js RSC / server-action wire response.

    The format is two newline-separated lines:
        0:{"a":"$@1","f":"","b":"..."}
        1:<actual JSON payload>
    """
    for line in text.splitlines():
        if line.startswith("1:"):
            return json.loads(line[2:])
    raise ValueError(f"Could not find payload line in RSC response:\n{text[:200]}")


# ── ApiClient ─────────────────────────────────────────────────────────────────


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
        self._action_cache: dict[str, str] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        return await self._auth.get_session()

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _discover_action_ids(self) -> dict[str, str]:
        """
        Fetch the energy page, find all Next.js chunk URLs, download them
        concurrently, and extract ``createServerReference`` action IDs.

        Results are cached for the lifetime of the instance.
        """
        if self._action_cache:
            return self._action_cache

        session = await self._get_session()
        async with session.get(ENERGY_URL) as resp:
            resp.raise_for_status()
            html = await resp.text()

        chunk_urls: list[str] = re.findall(
            r'src="(https://assets\.coolblue\.nl[^"]+_next/static/chunks[^"]+\.js)"',
            html,
        )
        logger.debug("Scanning %d JS chunks for action IDs …", len(chunk_urls))

        timeout = aiohttp.ClientTimeout(total=15)

        async def _scan_chunk(url: str) -> dict[str, str]:
            try:
                async with session.get(url, timeout=timeout) as r:
                    if not r.ok:
                        return {}
                    text = await r.text()
                    return {fn: aid for aid, fn in _SERVER_ACTION_RE.findall(text)}
            except Exception as exc:
                logger.warning("Could not fetch chunk %s: %s", url, exc)
                return {}

        results = await asyncio.gather(*(_scan_chunk(url) for url in chunk_urls))
        ids: dict[str, str] = {}
        for result in results:
            ids.update(result)

        if ids:
            logger.debug("Discovered %d action IDs: %s", len(ids), sorted(ids.keys()))
        else:
            logger.warning("No action IDs found in %d chunks", len(chunk_urls))

        self._action_cache = ids
        return ids

    async def _action_id(self, fn_name: str) -> str:
        """Return the current action ID for *fn_name*, raising if not found."""
        action = (await self._discover_action_ids()).get(fn_name)
        if action:
            return action
        raise RuntimeError(
            f"Action '{fn_name}' not found in discovered IDs. "
            "The page may have been redeployed – clear the action cache and retry."
        )

    async def _next_action_post(self, next_action: str, payload: list) -> str:
        """POST a Next.js server action and return the raw response text."""
        session = await self._get_session()
        async with session.post(
            ENERGY_URL,
            data=json.dumps(payload),
            headers={
                "Accept": "text/x-component",
                "Content-Type": "text/plain;charset=UTF-8",
                "next-action": next_action,
                "next-router-state-tree": _NEXT_ROUTER_STATE_TREE,
            },
        ) as r:
            r.raise_for_status()
            return await r.text()

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

        if not debtor or not location:
            raise RuntimeError(
                "Could not find debtorNumber / locationId in energy page RSC.\n"
                f"debtor={debtor}, location={location}\n"
                f"Page length: {len(html)}, RSC chunks: {len(chunks)}"
            )

        return debtor.group(1), location.group(1)

    async def get_hourly_energy(
        self, request: GetMeterReadingsRequest
    ) -> list[MeterReadingEntry]:
        """
        Fetch hourly energy data for the date specified in *request*.

        Calls the ``getMeterReadings`` Next.js server action and returns the
        response parsed into typed :class:`~model.MeterReadingEntry` objects.
        """
        action = await self._action_id("getMeterReadings")
        raw = await self._next_action_post(action, request.to_payload())
        return _MeterReadingList.validate_python(_parse_rsc_response(raw))

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        await self._auth.close()

    async def __aenter__(self) -> ApiClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

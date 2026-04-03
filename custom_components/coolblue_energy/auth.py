"""
auth.py

OIDC authentication for the Coolblue portal.

Two rounds are required to obtain both session cookies:
  Round 1 – standard email/password login  → sets ``Coolblue-Session``
  Round 2 – energy-page redirect flow      → sets ``Secure-Coolblue``
"""

from __future__ import annotations

import logging
import secrets

import aiohttp
from bs4 import BeautifulSoup
from yarl import URL

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

_ACCOUNTS_BASE = "https://accounts.coolblue.nl"

_AUTH_BASE = (
    f"{_ACCOUNTS_BASE}/connect/authorize"
    "?authentication_state=Unknown"
    "&client_id=Webshop"
    "&redirect_uri=https%3A%2F%2Fwww.coolblue.nl%2Fen%2Flogin%2Foidc"
    "&response_type=code"
    "&scope=openid+email+profile+offline_access"
    "+openid%3Acustomerid+openid%3Aidentityroleid"
    "+ucp%3Ascopes%3Acheckout_session+openid"
    "&ui_locales=en-US+en"
)

_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) Gecko/20100101 Firefox/149.0"


def _auth_url() -> str:
    """Build an OIDC authorize URL with fresh state and nonce values."""
    state = secrets.token_hex(16)
    nonce = secrets.token_hex(16)
    return f"{_AUTH_BASE}&nonce={nonce}&state={state}"


def _get_csrf(html: str, view: str) -> str:
    """
    Extract the CSRF JWT from the form that contains ``view=<view>``.
    Falls back to the first csrf input on the page.
    """
    soup = BeautifulSoup(html, "html.parser")
    view_input = soup.find("input", {"name": "view", "value": view})
    if view_input:
        form = view_input.find_parent("form")
        if form:
            csrf_input = form.find("input", {"name": "csrf"})
            if csrf_input:
                return csrf_input["value"]  # type: ignore[return-value]

    csrf_input = soup.find("input", {"name": "csrf"})
    if not csrf_input:
        raise ValueError(f"No CSRF token found on page (looking for view={view!r})")
    return csrf_input["value"]  # type: ignore[return-value]


# ── AuthService ───────────────────────────────────────────────────────────────


class AuthService:
    """
    Async OIDC authentication for the Coolblue Energy portal.

    Usage::

        auth = AuthService("you@example.com", "secret")
        session = await auth.get_session()   # authenticates lazily
        await auth.close()                   # or use as async context manager

        async with AuthService("you@example.com", "secret") as auth:
            session = await auth.get_session()
    """

    def __init__(self, email: str, password: str, energy_url: str) -> None:
        self._email = email
        self._password = password
        self._energy_url = energy_url
        self._session: aiohttp.ClientSession | None = None

    async def get_session(self) -> aiohttp.ClientSession:
        """Return the authenticated session, authenticating lazily if needed."""
        if self._session is None or self._session.closed:
            await self.authenticate()
        return self._session  # type: ignore[return-value]

    async def authenticate(self) -> None:
        """
        Perform the full two-round OIDC login flow and store the session.

        :raises aiohttp.ClientResponseError: on non-2xx HTTP responses
        :raises RuntimeError:                on credential or flow errors
        """
        if self._session and not self._session.closed:
            await self._session.close()

        session = aiohttp.ClientSession(
            headers={
                "User-Agent": _USER_AGENT,
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

        try:
            logger.debug("Auth round 1: establish Coolblue-Session")
            await self._oidc_round(session, _auth_url())

            # Round 2: GET energy page → 307 → /en/login?returnUrl=… → accounts URL
            # That accounts URL carries returnUrl in its state, causing the OIDC
            # callback to also issue Secure-Coolblue.
            logger.debug("Auth round 2: obtain Secure-Coolblue")
            async with session.get(self._energy_url, allow_redirects=False) as r:
                if r.status == 307:
                    loc = r.headers.get("Location", "")
                    if loc.startswith("/"):
                        loc = "https://www.coolblue.nl" + loc
                    async with session.get(loc, allow_redirects=False) as r2:
                        if r2.status in (301, 302, 303, 307, 308):
                            accounts_url = r2.headers["Location"]
                            logger.debug("Round 2 accounts URL: %s", accounts_url[:80])
                            await self._oidc_round(session, accounts_url)

        except Exception:
            await session.close()
            raise

        cookies = session.cookie_jar.filter_cookies(URL(self._energy_url))
        if "Secure-Coolblue" not in cookies:
            logger.warning("Secure-Coolblue was not set – energy API calls may fail")

        logger.info("Authentication successful. Cookies: %s", list(cookies.keys()))
        self._session = session

    async def _oidc_round(self, session: aiohttp.ClientSession, auth_url: str) -> None:
        """Complete one OIDC email + password round starting from *auth_url*."""
        logger.debug("OIDC round: GET %s", auth_url[:80])
        async with session.get(auth_url) as r:
            r.raise_for_status()
            html = await r.text()
            page_url = str(r.url)

        csrf = _get_csrf(html, "email-exists")
        async with session.post(
            page_url,
            data={"view": "email-exists", "csrf": csrf, "username": self._email},
        ) as r:
            r.raise_for_status()
            html = await r.text()
            page_url = str(r.url)

        csrf = _get_csrf(html, "login")
        async with session.post(
            page_url,
            data={
                "view": "login",
                "csrf": csrf,
                "username": self._email,
                "password": self._password,
            },
        ) as r:
            r.raise_for_status()
            if "accounts.coolblue.nl" in str(r.url):
                raise RuntimeError(
                    "OIDC round failed – still on accounts page. Check credentials."
                )

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> AuthService:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

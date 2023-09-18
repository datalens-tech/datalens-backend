import logging
import time
from typing import (
    Optional,
    Tuple,
)

import aiohttp

from dl_utils.aio import await_sync

LOGGER = logging.getLogger(__name__)


LOCAL_METADATA_TOKEN_URL = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"


async def get_yc_service_token_local_details(url: str = LOCAL_METADATA_TOKEN_URL) -> Tuple[str, int]:
    """Request the local metadata and return IAM token and its TTL (in seconds)"""
    LOGGER.debug("Going to get SA IAM token from the local metadata")
    async with aiohttp.ClientSession() as session:
        req = session.request(
            method="GET",
            url=url,
            headers={"Metadata-Flavor": "Google"},
            allow_redirects=False,
        )
        async with req as resp:
            if resp.status != 200:
                body = await resp.text("utf-8", errors="replace")
                raise Exception("Non-ok localhost metadata response", dict(body=body))
            # Example response text:
            #     {"access_token":"t1.9eud…","expires_in":43166,"token_type":"Bearer"}
            token_resp_data = await resp.json()

    token_type = token_resp_data.get("token_type")
    if token_type and token_type != "Bearer":
        raise ValueError(f"Unexpected token type {token_type!r}, must be 'Bearer'")

    access_token = token_resp_data["access_token"]
    assert isinstance(access_token, str)
    expires_in = token_resp_data["expires_in"]
    assert isinstance(expires_in, int)
    return access_token, expires_in


class LocalTokenCacheSingleton:
    token: Optional[str] = None
    expires_at: float = 0
    expires_at_absolute: float = 0

    @classmethod
    async def get_token(cls, min_ttl_sec: float = 900.0) -> str:
        now = time.monotonic()
        now_abs = time.time()
        if cls.token is not None and cls.expires_at > now + min_ttl_sec:
            return cls.token
        token, expires_in = await get_yc_service_token_local_details()
        if expires_in < min_ttl_sec * 2:  # fresh token is not fresh enough
            LOGGER.warning(f"LocalTokenCacheSingleton: {min_ttl_sec=!r} is too low, {expires_in=!r}, {now_abs=!r}")
        cls.expires_at = now + expires_in  # err to the earlier expiration
        cls.expires_at_absolute = now_abs + expires_in
        cls.token = token
        return token


# Note: add args here as needed.
async def get_yc_service_token_local() -> str:
    return await LocalTokenCacheSingleton.get_token()


def get_yc_service_token_local_sync() -> str:
    return await_sync(get_yc_service_token_local())

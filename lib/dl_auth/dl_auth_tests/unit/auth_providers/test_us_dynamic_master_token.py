import jwt
import pytest

import dl_auth
from dl_auth.auth_providers import (
    USDynamicMasterTokenAuthProvider,
    USDynamicMasterTokenAuthProviderSettings,
)


def _build_settings(private_key: str) -> USDynamicMasterTokenAuthProviderSettings:
    settings = dl_auth.AuthProviderSettings.factory(
        {
            "type": "US_DYNAMIC_MASTER_TOKEN",
            "PRIVATE_KEY": private_key,
        }
    )
    assert isinstance(settings, USDynamicMasterTokenAuthProviderSettings)
    return settings


def test_settings_registered(private_key: str) -> None:
    settings = _build_settings(private_key)
    assert isinstance(settings, USDynamicMasterTokenAuthProviderSettings)


def test_from_settings(private_key: str) -> None:
    provider = USDynamicMasterTokenAuthProvider.from_settings(_build_settings(private_key))
    assert isinstance(provider, USDynamicMasterTokenAuthProvider)


def test_get_headers_emits_dynamic_token(private_key: str, public_key: str) -> None:
    provider = USDynamicMasterTokenAuthProvider.from_settings(_build_settings(private_key))

    headers = provider.get_headers()

    assert set(headers.keys()) == {"X-US-Dynamic-Master-Token"}
    payload = jwt.decode(headers["X-US-Dynamic-Master-Token"], public_key, algorithms=["RS256"])
    assert payload["serviceId"] == "bi"
    assert payload["exp"] - payload["iat"] == 3600


def test_get_headers_caches_token(private_key: str) -> None:
    provider = USDynamicMasterTokenAuthProvider.from_settings(_build_settings(private_key))
    assert provider.get_headers() == provider.get_headers()


def test_get_cookies(private_key: str) -> None:
    provider = USDynamicMasterTokenAuthProvider.from_settings(_build_settings(private_key))
    assert provider.get_cookies() == {}


@pytest.mark.asyncio
async def test_get_headers_async_matches_sync(private_key: str, public_key: str) -> None:
    provider = USDynamicMasterTokenAuthProvider.from_settings(_build_settings(private_key))

    sync_headers = provider.get_headers()
    async_headers = await provider.get_headers_async()

    assert async_headers == sync_headers
    assert set(async_headers.keys()) == {"X-US-Dynamic-Master-Token"}
    payload = jwt.decode(async_headers["X-US-Dynamic-Master-Token"], public_key, algorithms=["RS256"])
    assert payload["serviceId"] == "bi"


@pytest.mark.asyncio
async def test_get_cookies_async(private_key: str) -> None:
    provider = USDynamicMasterTokenAuthProvider.from_settings(_build_settings(private_key))
    assert await provider.get_cookies_async() == {}

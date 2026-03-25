from __future__ import annotations

from cryptography import fernet
import pytest

from dl_api_commons.base_models import (
    RequestContextInfo,
    TenantCommon,
)
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.services_registry.top_level import (
    DummyServiceRegistry,
    ServicesRegistry,
)
import dl_crypto


@pytest.fixture
def crypto_keys_config() -> CryptoKeysConfig:
    return CryptoKeysConfig(
        map_id_key=dict(
            test_key=fernet.Fernet.generate_key().decode("ascii"),
        ),
        actual_key_id="test_key",
    )


@pytest.fixture
def crypto_controller(crypto_keys_config) -> dl_crypto.CryptoController:
    return dl_crypto.CryptoController(crypto_keys_config)


@pytest.fixture
def bi_context() -> RequestContextInfo:
    return RequestContextInfo.create(
        request_id=None,
        tenant=TenantCommon(),
        user_id=None,
        user_name=None,
        x_dl_debug_mode=False,
        endpoint_code=None,
        x_dl_context=None,
        plain_headers={},
        secret_headers={},
        auth_data=None,
    )


@pytest.fixture
def default_service_registry(bi_context) -> ServicesRegistry:
    return DummyServiceRegistry(rci=bi_context)

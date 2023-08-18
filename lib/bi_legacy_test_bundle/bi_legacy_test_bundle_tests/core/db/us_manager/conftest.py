from __future__ import annotations

import pytest

from bi_core.united_storage_client import USAuthContextMaster
from bi_core.us_manager.us_manager_sync import SyncUSManager


@pytest.fixture(scope='function')
def sync_usm(bi_context, default_service_registry, core_test_config):
    us_config = core_test_config.get_us_config()
    return SyncUSManager(
        us_base_url=us_config.us_host,
        us_auth_context=USAuthContextMaster(us_config.us_master_token),
        crypto_keys_config=core_test_config.get_crypto_keys_config(),
        bi_context=bi_context,
        services_registry=default_service_registry,
    )

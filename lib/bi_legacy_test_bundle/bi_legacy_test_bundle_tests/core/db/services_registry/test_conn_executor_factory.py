from __future__ import annotations

import pytest

from dl_core.services_registry import DefaultServicesRegistry


@pytest.mark.asyncio
async def test_caches(default_async_service_registry, saved_ch_connection):
    sr: DefaultServicesRegistry = default_async_service_registry
    ce_factory = sr.get_conn_executor_factory()

    initial_ce = ce_factory.get_async_conn_executor(saved_ch_connection)
    ce_wo_changes = ce_factory.get_async_conn_executor(saved_ch_connection)

    assert initial_ce is ce_wo_changes

    # Modifying connection
    saved_ch_connection.data.host = "example.com"
    ce_after_connection_change = ce_factory.get_async_conn_executor(saved_ch_connection)
    assert ce_after_connection_change is not initial_ce

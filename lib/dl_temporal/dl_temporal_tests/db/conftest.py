from typing import AsyncGenerator

import pytest
import pytest_asyncio

import dl_temporal
import dl_testing


@pytest.fixture(name="temporal_hostport")
def temporal_hostport() -> dl_testing.HostPort:
    hostport = dl_testing.get_test_container_hostport("temporal")
    dl_testing.wait_for_port(hostport.host, hostport.port)

    return hostport


@pytest.fixture(name="temporal_client_settings")
def temporal_client_settings(
    temporal_hostport: dl_testing.HostPort,
) -> dl_temporal.TemporalClientSettings:
    return dl_temporal.TemporalClientSettings(
        host=temporal_hostport.host,
        port=temporal_hostport.port,
        namespace="dl_temporal_tests",
    )


@pytest_asyncio.fixture(name="temporal_client")
async def temporal_client(
    temporal_client_settings: dl_temporal.TemporalClientSettings,
) -> AsyncGenerator[dl_temporal.TemporalClient, None]:
    result = await dl_temporal.TemporalClient.from_settings(temporal_client_settings)
    try:
        yield result
    finally:
        await result.close()

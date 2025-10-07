from typing import AsyncGenerator

import pytest
import pytest_asyncio

import dl_temporal
import dl_testing


@pytest.fixture(name="temporal_client_settings")
def temporal_client_settings() -> dl_temporal.TemporalClientSettings:
    hostport = dl_testing.get_test_container_hostport("temporal")

    return dl_temporal.TemporalClientSettings(
        host=hostport.host,
        port=hostport.port,
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

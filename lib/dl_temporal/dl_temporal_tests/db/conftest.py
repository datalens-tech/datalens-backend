import datetime
from typing import AsyncGenerator

import pytest
import pytest_asyncio

import dl_temporal
import dl_testing


@pytest.fixture(name="temporal_hostport")
def fixture_temporal_hostport() -> dl_testing.HostPort:
    hostport = dl_testing.get_test_container_hostport("temporal")
    dl_testing.wait_for_port(hostport.host, hostport.port)

    return hostport


@pytest.fixture(name="temporal_namespace")
def fixture_temporal_namespace() -> str:
    return "dl_temporal_tests"


@pytest.fixture(name="temporal_client_settings")
def fixture_temporal_client_settings(
    temporal_hostport: dl_testing.HostPort,
    temporal_namespace: str,
) -> dl_temporal.TemporalClientSettings:
    return dl_temporal.TemporalClientSettings(
        host=temporal_hostport.host,
        port=temporal_hostport.port,
        namespace=temporal_namespace,
    )


@pytest_asyncio.fixture(name="temporal_client")
async def fixture_temporal_client(
    temporal_client_settings: dl_temporal.TemporalClientSettings,
    temporal_namespace: str,
) -> AsyncGenerator[dl_temporal.TemporalClient, None]:
    client = await dl_temporal.TemporalClient.from_settings(temporal_client_settings)

    try:
        await client.register_namespace(
            namespace=temporal_namespace,
            workflow_execution_retention_period=datetime.timedelta(days=1),
        )
    except dl_temporal.AlreadyExists:
        pass

    try:
        yield client
    finally:
        await client.close()

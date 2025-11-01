import datetime
import logging
from typing import AsyncGenerator

import pytest
import pytest_asyncio

import dl_temporal
import dl_testing
import dl_utils


@pytest.fixture(name="temporal_hostport")
def fixture_temporal_hostport() -> dl_testing.HostPort:
    hostport = dl_testing.get_test_container_hostport("temporal")
    dl_testing.wait_for_port(
        host=hostport.host,
        port=hostport.port,
        timeout_seconds=30,
    )

    return hostport


@pytest.fixture(name="temporal_ui_hostport")
def fixture_temporal_ui_hostport() -> dl_testing.HostPort:
    hostport = dl_testing.get_test_container_hostport(service_key="temporal-ui", dc_filename="docker-compose-dev.yml")

    return hostport


@pytest.fixture(name="temporal_namespace")
def fixture_temporal_namespace() -> str:
    return "dl_temporal_tests"


@pytest_asyncio.fixture(name="temporal_client")
async def fixture_temporal_client(
    temporal_namespace: str,
    temporal_hostport: dl_testing.HostPort,
) -> AsyncGenerator[dl_temporal.TemporalClient, None]:
    client = await dl_temporal.TemporalClient.from_settings(
        dl_temporal.TemporalClientSettings(
            host=temporal_hostport.host,
            port=temporal_hostport.port,
            namespace=temporal_namespace,
            lazy=False,
        )
    )

    try:
        await client.register_namespace(
            namespace=temporal_namespace,
            workflow_execution_retention_period=datetime.timedelta(days=1),
        )
    except dl_temporal.AlreadyExists:
        pass

    await dl_utils.await_for(
        name="temporal client",
        condition=client.check_health,
        timeout=30,
        interval=1,
        log_func=logging.info,
    )

    try:
        yield client
    finally:
        await client.close()

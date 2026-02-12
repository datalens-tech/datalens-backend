import datetime
import logging
from typing import AsyncGenerator

import pytest
import pytest_asyncio
import temporalio.api
import temporalio.api.enums.v1

import dl_temporal
import dl_testing
import dl_utils


@pytest.fixture(name="temporal_hostport")
def fixture_temporal_hostport() -> dl_testing.HostPort:
    hostport = dl_testing.get_test_container_hostport("temporal")
    dl_testing.wait_for_port(
        host=hostport.host,
        port=hostport.port,
        timeout_seconds=60,
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
    client = await dl_temporal.TemporalClient.from_dependencies(
        dl_temporal.TemporalClientDependencies(
            host=temporal_hostport.host,
            port=temporal_hostport.port,
            namespace=temporal_namespace,
            tls=False,
            lazy=False,
        )
    )

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


@pytest_asyncio.fixture(name="register_namespace", autouse=True)
async def fixture_register_namespace(
    temporal_client: dl_temporal.TemporalClient,
    temporal_namespace: str,
) -> None:
    try:
        await temporal_client.register_namespace(
            namespace=temporal_namespace,
            workflow_execution_retention_period=datetime.timedelta(days=1),
        )
    except dl_temporal.AlreadyExists:
        pass


@pytest_asyncio.fixture(autouse=True)
async def fixture_add_search_attributes(
    temporal_client: dl_temporal.TemporalClient,
    register_namespace: None,
) -> None:
    await temporal_client.add_search_attributes(
        search_attributes={
            dl_temporal.base.SearchAttribute.RESULT_TYPE.value: temporalio.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
            dl_temporal.base.SearchAttribute.RESULT_CODE.value: temporalio.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
        },
    )

import asyncio
import time

import docker.errors
import pytest

from bi_testing_db_provision.dao.resource_dao import ResourceDAO
from bi_testing_db_provision.model.enums import ResourceState
from bi_testing_db_provision.model.request_db import DBRequest
from bi_testing_db_provision.model.resource_docker import StandalonePostgresResource, DockerResourceBase
from bi_testing_db_provision.workers.brigadier import Brigadier

pytestmark = pytest.mark.usefixtures("tdbp_clean_resources")


@pytest.mark.asyncio
async def test_worker(tdbp_pg_conn_factory, tdbp_brigadier: Brigadier, docker_client_per_func):
    docker_client = docker_client_per_func
    conn = await tdbp_pg_conn_factory.get()

    initial_resource = StandalonePostgresResource.create(
        resource_id=None,
        state=ResourceState.create_required,
        request=DBRequest(
            version='9.3',
        ),
    )

    rm = ResourceDAO(conn)
    await asyncio.sleep(1)
    async with conn.begin():
        initial_resource = await rm.create(initial_resource)

    ts_resource_inserted = time.monotonic()

    while True:
        await asyncio.sleep(1)
        reloaded_resource = await rm.get_by_id(initial_resource.id)
        if reloaded_resource.state == ResourceState.free:
            break
        if time.monotonic() - ts_resource_inserted > 10:
            pytest.fail("Resource creation timeout")

    # Checking that containers was created
    assert isinstance(reloaded_resource, DockerResourceBase)
    assert reloaded_resource.internal_data.container_name_map is not None

    for container_name in reloaded_resource.internal_data.container_name_map.values():
        container = docker_client.containers.get(container_name)
        assert container.attrs['State']['Status'] == 'running'

    async with conn.begin():
        await rm.update(reloaded_resource.clone(state=ResourceState.recycle_required))

    while True:
        await asyncio.sleep(1)
        reloaded_resource = await rm.get_by_id(initial_resource.id)
        if reloaded_resource.state == ResourceState.deleted:
            break
        if time.monotonic() - ts_resource_inserted > 10:
            pytest.fail("Resource deletion timeout")

    # Checking that containers was created
    assert isinstance(reloaded_resource, DockerResourceBase)
    assert reloaded_resource.internal_data.container_name_map is not None

    for container_name in reloaded_resource.internal_data.container_name_map.values():
        with pytest.raises(docker.errors.NotFound):
            docker_client.containers.get(container_name)

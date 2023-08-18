from __future__ import annotations

import aiohttp.pytest_plugin
import aiohttp.test_utils
import docker
import docker.errors
import pytest
import shortuuid
from docker import DockerClient
from sqlalchemy import create_engine

from bi_testing.utils import skip_outside_devhost
from bi_testing_db_provision.app import create_app
from bi_testing_db_provision.db.base import Base
from bi_testing_db_provision.db.resource import ResourceRecord
from bi_testing_db_provision.db_connection import DBConnFactory
from bi_testing_db_provision.db_utils import create_sa_engine_url
from bi_testing_db_provision.settings import DEVELOPMENT_SETTINGS
from bi_testing_db_provision.workers.brigadier import Brigadier
from bi_testing_db_provision.workers.worker_resource_docker import ResourceProvisioningWorker, ResourceRecyclingWorker

try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


@pytest.fixture(scope='session')
def clear_db():
    pg_config = DEVELOPMENT_SETTINGS.pg_config
    engine = create_engine(create_sa_engine_url(pg_config))
    Base.metadata.drop_all(engine)  # to ensure an empty db
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(scope='function')
def web_app(loop, aiohttp_client):
    return loop.run_until_complete(aiohttp_client(create_app(settings=DEVELOPMENT_SETTINGS)))


@pytest.fixture(scope='function')
@skip_outside_devhost
def docker_client_per_func() -> DockerClient:
    return docker.from_env()


@pytest.fixture()
@skip_outside_devhost
def removable_after_test_container_name(docker_client_per_func):
    container_name = f'tdbp_{shortuuid.uuid()}'
    yield container_name
    try:
        container = docker_client_per_func.containers.get(container_name)
    except docker.errors.NotFound:
        pass
    else:
        container.remove(v=True, force=True)


@pytest.fixture()
async def tdbp_pg_conn_factory(clear_db) -> DBConnFactory:
    conn_factory = await DBConnFactory.from_pg_config(
        pg_config=DEVELOPMENT_SETTINGS.pg_config
    )
    yield conn_factory
    await conn_factory.close()


@pytest.fixture()
async def tdbp_clean_resources(tdbp_pg_conn_factory) -> None:
    conn = await tdbp_pg_conn_factory.get()
    await conn.execute(f"TRUNCATE TABLE {ResourceRecord.__tablename__}")


@pytest.fixture()
async def tdbp_brigadier(request, tdbp_pg_conn_factory) -> Brigadier:
    requested_worker_config = request.param if hasattr(request, 'param') else None
    if requested_worker_config is None:
        effective_worker_config = {
            ResourceProvisioningWorker: 1,
            ResourceRecyclingWorker: 1,
        }
    else:
        effective_worker_config = requested_worker_config

    brigadier = Brigadier(
        worker_id_prefix="tdbp_brigadier_fixture",
        initial_worker_config=effective_worker_config,
        conn_factory=tdbp_pg_conn_factory,
    )
    await brigadier.launch_workers()
    yield brigadier
    await brigadier.stop_workers()

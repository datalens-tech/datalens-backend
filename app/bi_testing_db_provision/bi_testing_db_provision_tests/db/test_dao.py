import contextlib
import uuid
from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest

from bi_testing_db_provision.model.base import BaseStoredModel
from bi_testing_db_provision.model.commons_db import DBCreds
from bi_testing_db_provision.model.enums import ResourceState, SessionState
from bi_testing_db_provision.model.request_db import DBRequest
from bi_testing_db_provision.model.resource_data_db import ResourceExternalDataDBDefaultSQL
from bi_testing_db_provision.model.resource_docker import StandalonePostgresResource, DockerResourceInternalData
from bi_testing_db_provision.model.session import Session
from bi_testing_db_provision.dao.base_dao import BaseDAO
from bi_testing_db_provision.dao.resource_dao import ResourceDAO
from bi_testing_db_provision.dao.session_dao import SessionDAO


class BaseExecutorTestSet:
    @pytest.fixture()
    def obj_to_save(self) -> BaseStoredModel:
        pass

    @pytest.fixture()
    async def dao(self) -> BaseDAO:
        raise NotImplementedError()

    @pytest.mark.asyncio
    async def test_save(self, dao, obj_to_save):
        assert obj_to_save.id is None
        saved_obj = await dao.create(obj_to_save)
        assert isinstance(saved_obj.id, UUID)

        try:
            reloaded_obj = await dao.get_by_id(saved_obj.id)
        finally:
            await dao.delete_by_id(saved_obj.id)

        assert reloaded_obj == saved_obj


class TestSessionDAO(BaseExecutorTestSet):
    @pytest.fixture()
    async def dao(self, tdbp_pg_conn_factory) -> BaseDAO:
        conn = await tdbp_pg_conn_factory.get()
        return SessionDAO(conn)

    @pytest.fixture()
    def obj_to_save(self) -> BaseStoredModel:
        return Session(
            id=None,
            state=SessionState.active,
            description='Some test session',
        )


class TestResourceDAO(BaseExecutorTestSet):
    @pytest.fixture()
    def dao_factory(self, tdbp_pg_conn_factory) -> Callable[[], Awaitable[ResourceDAO]]:
        async def create_dao() -> ResourceDAO:
            conn = await tdbp_pg_conn_factory.get()
            return ResourceDAO(conn)

        return create_dao

    @pytest.fixture()
    async def dao(self, dao_factory) -> BaseDAO:
        return await dao_factory()

    @pytest.fixture()
    def obj_to_save(self) -> BaseStoredModel:
        return StandalonePostgresResource.create(
            resource_id=None,
            state=ResourceState.free,
            request=DBRequest(),
            external_data=ResourceExternalDataDBDefaultSQL(
                db_host='localhost',
                db_port=8080,
                db_names=('a', 'b'),
                creds=(DBCreds('user', 'password'),),
            ),
            internal_data=DockerResourceInternalData(
                allocation_host='localhost',
                container_name_map=dict(main='tdb_123123'),
            ),
        )

    @pytest.mark.asyncio
    async def test_lock(self, dao_factory):
        dao_1 = await dao_factory()
        dao_2 = await dao_factory()

        conn_1 = dao_1.conn
        conn_2 = dao_2.conn
        assert conn_1 is not conn_2

        initial_resource = StandalonePostgresResource.create(
            resource_id=None,
            state=ResourceState.free,
            request=DBRequest(
                version=uuid.uuid4().hex,
            ),
        )

        initial_resource = await dao_1.create(initial_resource)

        exit_stack = contextlib.AsyncExitStack()

        await exit_stack.enter_async_context(conn_1.begin())
        await exit_stack.enter_async_context(conn_2.begin())

        async with exit_stack:
            locked_resource_1 = await dao_1.find_and_lock_free_resource(initial_resource.type, initial_resource.request)
            locked_resource_2 = await dao_2.find_and_lock_free_resource(initial_resource.type, initial_resource.request)

            assert locked_resource_1.id == initial_resource.id
            assert locked_resource_2 is None

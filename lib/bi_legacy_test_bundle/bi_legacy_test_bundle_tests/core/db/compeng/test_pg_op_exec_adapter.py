from __future__ import annotations

import abc
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List

import aiopg.sa
import asyncpg
import pytest

from dl_constants.enums import BIType

from dl_compeng_pg.compeng_pg_base.exec_adapter_base import PostgreSQLExecAdapterAsync
from dl_compeng_pg.compeng_aiopg.exec_adapter_aiopg import AiopgExecAdapter
from dl_compeng_pg.compeng_asyncpg.exec_adapter_asyncpg import AsyncpgExecAdapter
from dl_core.data_processing.streaming import AsyncChunked
from dl_core.utils import make_id


async def get_active_queries(pg_adapter: PostgreSQLExecAdapterAsync) -> List[Dict[str, Any]]:
    columns = '''
    datid datname pid usesysid usename application_name
    client_addr client_hostname client_port
    backend_start xact_start query_start
    state_change state query'''.split()
    active_queries_query = (
        'SELECT {cols} FROM pg_stat_activity  -- active_queries_query'.format(
            cols=', '.join(columns)))
    queries_resp = await pg_adapter.fetch_data_from_select(
        query=active_queries_query,
        user_types=[BIType.string] * len(columns),
        chunk_size=100,
        query_id=make_id(),
    )
    queries = await queries_resp.all()
    queries = [dict(zip(columns, row)) for row in queries]  # should've perhaps been in the cursor?
    # skip self
    queries = [
        query
        for query in queries
        if query['query'] and 'active_queries_query' not in query['query']]
    return queries


class BaseTestPGOpExecAdapter(metaclass=abc.ABCMeta):
    min_size: int = 1
    max_size: int = 5

    @pytest.fixture(scope='function')
    async def pg_adapter(self, loop, compeng_pg_dsn) -> AsyncGenerator[PostgreSQLExecAdapterAsync, None]:
        async with self.make_pg_adapter(compeng_pg_dsn=compeng_pg_dsn) as pg_adapter:
            yield pg_adapter

    @asynccontextmanager
    @abc.abstractmethod
    async def make_pg_adapter(self, compeng_pg_dsn) -> AsyncGenerator[PostgreSQLExecAdapterAsync, None]:
        raise NotImplementedError

    async def table_exists(self, pg_adapter: PostgreSQLExecAdapterAsync, table_name: str) -> bool:
        query = f"SELECT EXISTS (SELECT table_name from information_schema.tables WHERE table_name = '{table_name}')"
        return await pg_adapter.scalar(query=query, user_type=BIType.boolean)

    @pytest.mark.asyncio
    async def test_create_drop_table(self, pg_adapter: PostgreSQLExecAdapterAsync):
        table_name = f'table_{uuid.uuid4()}'
        names = ['id', 'value']
        user_types = [BIType.integer, BIType.string]
        assert not await self.table_exists(pg_adapter=pg_adapter, table_name=table_name)
        await pg_adapter.create_table(table_name=table_name, names=names, user_types=user_types)
        assert await self.table_exists(pg_adapter=pg_adapter, table_name=table_name)
        await pg_adapter.drop_table(table_name=table_name)
        assert not await self.table_exists(pg_adapter=pg_adapter, table_name=table_name)

    @pytest.mark.asyncio
    async def test_insert_fetch(self, pg_adapter: PostgreSQLExecAdapterAsync):
        queries_before = await get_active_queries(pg_adapter)

        table_name = f'table_{uuid.uuid4()}'
        names = ['int_value', 'str_value', 'bool_value']
        user_types = [BIType.integer, BIType.string, BIType.boolean]

        raw_data = [
            # 5 chunks X 1000 rows
            [
                (i * 100, f'str_{i}', bool(i % 2))
                for i in range(j * 1000, j * 1000 + 1000)
            ]
            for j in range(5)
        ]
        data = AsyncChunked.from_chunked_iterable(raw_data)
        await pg_adapter.create_table(table_name=table_name, names=names, user_types=user_types)
        assert await self.table_exists(pg_adapter=pg_adapter, table_name=table_name)

        await pg_adapter.insert_data_into_table(
            table_name=table_name, names=names, user_types=user_types, data=data)
        fetched_data = await pg_adapter.fetch_data_from_select(
            query=f'SELECT {", ".join(names)} FROM "{table_name}" ORDER BY int_value',
            user_types=user_types, query_id=make_id(),
        )
        fetched_as_lists = [list(row) for row in await fetched_data.all()]
        original_as_lists = [list(row) for row in await AsyncChunked.from_chunked_iterable(raw_data).all()]
        assert fetched_as_lists == original_as_lists

        queries_after = await get_active_queries(pg_adapter)
        new_queries = [q for q in queries_after if q not in queries_before]
        assert not new_queries, "should have no new active queries"

        if not isinstance(pg_adapter, AsyncpgExecAdapter):
            # FIXME (asyncpg): cannot DROP TABLE "table_8df53688-8f9b-4a6a-a281-cdf954075f3a"
            #  because it is being used by active queries in this session
            await pg_adapter.drop_table(table_name=table_name)

    @pytest.mark.asyncio
    async def test_create_same_name_tables(self, loop, compeng_pg_dsn):
        """Check that it's possible to create the same table across sessions"""
        table_name = str(uuid.uuid4())
        attempts = 10
        assert attempts > self.max_size
        for attempt in range(attempts):
            async with self.make_pg_adapter(compeng_pg_dsn=compeng_pg_dsn) as pg_adapter:
                await pg_adapter.create_table(
                    table_name=table_name, names=['int_value'], user_types=[BIType.integer]
                )


class TestAiopgOpRunner(BaseTestPGOpExecAdapter):

    @asynccontextmanager
    async def make_pg_adapter(self, compeng_pg_dsn) -> AsyncGenerator[PostgreSQLExecAdapterAsync, None]:
        async with aiopg.sa.create_engine(
                compeng_pg_dsn, minsize=self.min_size, maxsize=self.max_size
        ) as engine:
            async with engine.acquire() as conn:
                yield AiopgExecAdapter(conn=conn)


class TestAsyncpgOpRunner(BaseTestPGOpExecAdapter):

    @asynccontextmanager
    async def make_pg_adapter(self, compeng_pg_dsn) -> AsyncGenerator[PostgreSQLExecAdapterAsync, None]:
        pool = await asyncpg.create_pool(
            compeng_pg_dsn, min_size=self.min_size, max_size=self.max_size
        )
        async with pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    yield AsyncpgExecAdapter(conn=conn)

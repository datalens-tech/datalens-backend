from __future__ import annotations

import attr
import pytest

from dl_constants.enums import BIType

from dl_core import exc
from dl_api_commons.base_models import RequestContextInfo
from dl_connector_clickhouse.core.clickhouse_base.adapters import AsyncClickHouseAdapter
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core_testing.database import make_table, C, DbTable

from dl_connector_clickhouse.core.clickhouse.testing.exec_factory import ClickHouseExecutorFactory


class TestAsyncClickHouseAdapter:
    exec_factory_cls = ClickHouseExecutorFactory

    @pytest.fixture()
    def db(self, clickhouse_db):
        return clickhouse_db

    @pytest.fixture()
    def conn_target_dto(self, db):
        return self.exec_factory_cls(db).make_dto()

    @pytest.fixture()
    def tbl(self, db) -> DbTable:
        tbl = make_table(db, rows=1000, columns=[
            C('str_val', BIType.string, vg=lambda rn, **kwargs: str(rn)),
            C('int_val', BIType.integer, vg=lambda rn, **kwargs: rn),
        ])
        yield tbl
        db.drop_table(tbl.table)

    @pytest.mark.asyncio
    async def test_timeout(self, conn_target_dto):
        rci = RequestContextInfo.create_empty()
        dba = AsyncClickHouseAdapter(
            req_ctx_info=rci,
            target_dto=attr.evolve(conn_target_dto, port='63545'),
            default_chunk_size=10,
        )
        with pytest.raises(exc.SourceConnectError):
            await dba.execute(DBAdapterQuery(query='select 1'))

    @pytest.mark.parametrize(
        "pass_db_query_to_user,expected_query",
        (
            (None, None),
            (False, None),
            (True, "select 1"),
        ),
    )
    @pytest.mark.asyncio
    async def test_pass_db_query_to_user(self, conn_target_dto, pass_db_query_to_user, expected_query):
        target_dto = conn_target_dto.clone(port="63545")
        if pass_db_query_to_user is not None:
            target_dto = target_dto.clone(pass_db_query_to_user=pass_db_query_to_user)

        rci = RequestContextInfo.create_empty()
        dba = AsyncClickHouseAdapter(
            req_ctx_info=rci,
            target_dto=target_dto,
            default_chunk_size=10,
        )

        with pytest.raises(exc.SourceConnectError) as exception_info:
            await dba.execute(DBAdapterQuery(query="select 1", debug_compiled_query="select 1"))

            assert exception_info.value.query == expected_query

    @pytest.mark.asyncio
    async def test_simple(self, tbl, conn_target_dto):
        rci = RequestContextInfo.create_empty()
        dba = AsyncClickHouseAdapter(
            req_ctx_info=rci,
            target_dto=conn_target_dto,
            default_chunk_size=10,
        )
        try:
            res = await dba.execute(DBAdapterQuery(
                query=f"SELECT * FROM {tbl.db.quote(tbl.name)}"
            ))

            all_data = []
            async for chunk in res.raw_chunk_generator:
                all_data.extend(chunk)

            assert len(all_data) == 1000
        finally:
            await dba.close()

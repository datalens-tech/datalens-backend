from __future__ import annotations

import pytest

from bi_core import exc
from bi_core import connection_executors
from bi_core.connectors.clickhouse_base.conn_options import CHConnectOptions
from bi_core.connectors.chydb.dto import CHOverYDBDTO
from bi_core.connectors.chydb.connection_executors import (
    CHYDBSyncAdapterConnExecutor, CHYDBAsyncAdapterConnExecutor,
)

from bi_legacy_test_bundle_tests.core.common_ce import ErrorTestSet, SelectDataTestSet
from bi_legacy_test_bundle_tests.core.common_ce_ch import CHLikeBaseTestSet

from bi_legacy_test_bundle_tests.core.ext.chydb.config import CHYDB_TEST_TABLE_SQL


class BaseCHYDBCETestSet(CHLikeBaseTestSet):

    @pytest.fixture()
    def db(self):
        raise NotImplementedError

    @pytest.fixture()
    def conn_dto(self, chydb_test_connection_params_base):
        return CHOverYDBDTO(
            conn_id=None,
            protocol='http',
            endpoint='',
            **chydb_test_connection_params_base)

    @pytest.fixture()
    def default_conn_options(self):
        return CHConnectOptions(max_execution_time=16)

    @pytest.fixture(params=['not_exists'])
    def error_test_set(self, request) -> ErrorTestSet:
        return {
            'not_exists': ErrorTestSet(
                query=connection_executors.ConnExecutorQuery(
                    "SELECT * FROM ydbTable('ru', '/ru/home/hhell/mydb', 'some_dir/test_table_e_nonexistent')"),
                expected_err_cls=exc.CHYDBQueryError,
                # TODO?: expected_err_cls=exc.SourceDoesNotExist,
            ),
        }[request.param]

    @pytest.fixture()
    def all_supported_types_test_case(self):
        raise pytest.skip('TODO: Not currently feasible because of TableIdent')  # TODO

    @pytest.mark.skip()
    @pytest.mark.asyncio
    async def test_table_exists_sync(self, executor, is_table_exists_test_case):
        raise pytest.skip('TODO: is_table_exists_test_case fixture contents')

    @pytest.mark.skip()
    @pytest.mark.asyncio
    async def test_table_exists(self, executor, is_table_exists_test_case):
        raise pytest.skip('TODO: is_table_exists_test_case fixture contents')

    @pytest.fixture()
    def select_data_test_set(self):
        return SelectDataTestSet(
            # table=None,
            query=connection_executors.ConnExecutorQuery(
                query="SELECT max(some_int64) FROM {}".format(CHYDB_TEST_TABLE_SQL),
                chunk_size=6,
            ),
            expected_data=[
                (4611686018427387904,),
            ]
        )

    @pytest.mark.skip("Probably not relevant for CHYDB")
    @pytest.mark.asyncio
    async def test_unauthorized_proxy_access_attempt(self, exec_mode, conn_dto, executor_builder):
        return await super().test_unauthorized_proxy_access_attempt(exec_mode, conn_dto, executor_builder)

    @pytest.mark.skip("Not relevant for CHYDB")
    def test_get_table_names_sync(self, sync_exec_wrapper, get_table_names_test_case):
        super().test_get_table_names_sync(sync_exec_wrapper, get_table_names_test_case)

    @pytest.mark.skip("Not relevant for CHYDB")
    @pytest.mark.asyncio
    async def test_get_table_names(self, executor, get_table_names_test_case):
        return await super().test_get_table_names(executor, get_table_names_test_case)


@pytest.mark.yt
class TestCHYDBCESyncAdapterConnExecutor(BaseCHYDBCETestSet):
    executor_cls = CHYDBSyncAdapterConnExecutor

    @pytest.mark.skip()
    async def test_explain(self, executor_builder, default_conn_options, caplog, exec_mode):
        pass


@pytest.mark.yt
class TestCHYDBCEAsyncAdapterConnExecutor(BaseCHYDBCETestSet):
    executor_cls = CHYDBAsyncAdapterConnExecutor
    inf_val = None

    @pytest.mark.skip()
    def test_type_discovery_sync(self, sync_exec_wrapper, all_supported_types_test_case):
        pass

    @pytest.mark.skip()
    @pytest.mark.asyncio
    async def test_get_db_version(self, executor):
        pass

    @pytest.mark.skip()
    def test_get_db_version_sync(self, sync_exec_wrapper):
        pass

    @pytest.mark.skip()
    @pytest.mark.asyncio
    async def test_get_table_names(self, executor, get_table_names_test_case):
        pass

    @pytest.mark.skip()
    def test_get_table_names_sync(self, sync_exec_wrapper, get_table_names_test_case):
        pass

    @pytest.mark.skip()
    @pytest.mark.asyncio
    async def test_table_exists(self, executor, is_table_exists_test_case):
        pass

    @pytest.mark.skip()
    def test_table_exists_sync(self, sync_exec_wrapper, is_table_exists_test_case):
        pass

    @pytest.mark.skip('CHYDB CH is too old')
    async def test_explain(self, executor_builder, default_conn_options, caplog, exec_mode):
        pass

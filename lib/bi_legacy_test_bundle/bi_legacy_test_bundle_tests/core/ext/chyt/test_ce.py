from __future__ import annotations

import asyncio
import functools
from typing import ClassVar, Type

import attr
import pytest

from bi_constants.enums import BIType, ConnectionType, IndexKind

from bi_core import exc
from bi_api_commons.base_models import RequestContextInfo
from bi_core.connection_executors import ConnExecutorQuery
from bi_core.connection_models import ConnDTO, TableIdent
from bi_core.db import SchemaInfo, IndexInfo

from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)
from bi_connector_chyt_internal.core.conn_options import CHYTInternalConnectOptions
from bi_connector_chyt_internal.core.connection_executors import (
    CHYTInternalSyncAdapterConnExecutor,
    CHYTUserAuthSyncAdapterConnExecutor,
    CHYTInternalAsyncAdapterConnExecutor,
    CHYTUserAuthAsyncAdapterConnExecutor,
)
from bi_connector_chyt_internal.core.dto import CHYTInternalDTO, CHYTUserAuthDTO

from bi_legacy_test_bundle_tests.core.common_ce import BaseConnExecutorSet, SelectDataTestSet, ErrorTestSet, ExecutorOptions
from bi_legacy_test_bundle_tests.core.common_ce_ch import CHLikeBaseTestSet

from bi_legacy_test_bundle_tests.core.fixtures_ce import (  # noqa: F401
    query_executor_app, query_executor_options, sync_remote_query_executor,
)


TEST_TABLE = "//home/yandexbi/datalens-back/bi_test_data/bi_398"
TEST_DATA = [
    (0, 1036, '2018-11-26', 1, 1029),
    (0, 1122, '2018-12-03', 1, 1112),
    (0, 1225, '2018-12-17', 1, 1155),
    (0, 2107, '2018-11-19', 1, 2059),
    (0, 2966, '2018-12-10', 1, 2878),
    (1, 3942, '2018-12-17', 1, 3791),
    (1, 15312, '2018-11-19', 1, 13964),
    (1, 15867, '2018-11-26', 1, 13863),
    (1, 18211, '2018-12-10', 1, 15472),
    (0, 20030, 'old', 1, 18909),
    (1, 24955, '2018-12-03', 1, 19029),
    (0, 58872, '2018-12-17', 0, 56049),
    (0, 96224, '2018-11-26', 0, 93303),
    (0, 99859, '2018-12-03', 0, 97240),
    (0, 126863, '2018-11-19', 0, 121507),
    (0, 169739, '2018-12-10', 0, 160878),
    (1, 272538, 'old', 1, 206009),
    (1, 399827, '2018-12-17', 0, 351731),
    (1, 1063024, '2018-12-10', 0, 836377),
    (1, 1086150, '2018-11-26', 0, 854692),
    (1, 1130505, '2018-11-19', 0, 896511),
    (1, 1385197, '2018-12-03', 0, 1005231),
    (0, 46113325, 'old', 0, 10210839),
    (1, 264463459, 'old', 0, 126335733),
]


@pytest.mark.yt
class BaseCHYTTestSet(CHLikeBaseTestSet):
    connection_type: ClassVar[Type[ConnectionType]]

    @pytest.fixture()
    def db(self):
        raise NotImplementedError()

    @pytest.fixture()
    def default_conn_options(self) -> CHYTInternalConnectOptions:
        return CHYTInternalConnectOptions(
            max_execution_time=100500,
        )

    @pytest.fixture(params=['not_exists', 'unschematized'])
    def error_test_set(self, request) -> ErrorTestSet:
        return {
            'not_exists': ErrorTestSet(
                query=ConnExecutorQuery("SELECT * FROM `//table_1234123_not_existing`"),
                expected_err_cls=exc.SourceDoesNotExist,
                expected_message_substring="table_1234123_not_existing",
            ),
            'unschematized': ErrorTestSet(
                query=ConnExecutorQuery("SELECT * FROM `//home/yandexbi/datalens-back/bi_test_data/unschematized`"),
                expected_err_cls=exc.CHYTTableHasNoSchema,
            )
        }[request.param]

    @pytest.fixture()
    def select_data_test_set(self):
        return SelectDataTestSet(
            # table=None,
            query=ConnExecutorQuery(
                query=f"""SELECT * FROM "{TEST_TABLE}" ORDER BY count""",
                chunk_size=6,
            ),
            expected_data=TEST_DATA,
        )

    # TODO FIX: Make table with all supported types
    @pytest.fixture()
    def all_supported_types_test_case(self):
        cd = functools.partial(self._ch_cd, ct=self.connection_type)
        col_data = [
            cd('hit_type', None, BIType.boolean, nullable=False, nt_name='ytboolean'),
            cd('count', None, BIType.integer, nullable=False, nt_name='uint64'),
            cd('week', None, BIType.string, nullable=True, nt_name='string'),
            cd('is_turbo', None, BIType.boolean, nullable=False, nt_name='ytboolean'),
            cd('count_distinct', None, BIType.integer, nullable=False, nt_name='uint64'),
        ]

        return self.TypeDiscoveryTestCase(
            table_ident=TableIdent(
                db_name=None,
                schema_name=None,
                table_name=TEST_TABLE,
            ),
            expected_schema_info=self.column_data_to_schema_info(col_data, self.connection_type)
        )

    @pytest.fixture(params=['no_sorting', 'one_col', 'two_col'])
    def index_test_case(self, request) -> 'TypeDiscoveryTestCase':
        test_data = dict(
            no_sorting=("//home/yandexbi/datalens-back/bi_test_data/bi_2132_no_sorting", None),
            one_col=("//home/yandexbi/datalens-back/bi_test_data/bi_2132_one_col_sorting", ('col_sort_1',)),
            two_col=("//home/yandexbi/datalens-back/bi_test_data/bi_2132_two_col_sorting", ('col_sort_2', 'col_sort_1',)),
        )
        table_path, cols = test_data[request.param]

        if cols is None:
            indexes = ()
        else:
            indexes = (IndexInfo(
                columns=cols,
                kind=IndexKind.table_sorting,
            ),)

        return self.TypeDiscoveryTestCase(
            table_ident=TableIdent(db_name=None, schema_name=None, table_name=table_path),
            expected_schema_info=SchemaInfo(
                schema=[],
                indexes=frozenset(indexes),
            )
        )

    @pytest.fixture(params=['not_exists', 'exists'])
    def is_table_exists_test_case(self, request) -> BaseConnExecutorSet.TableExistsTestCase:
        param = request.param
        if param == 'not_exists':
            yield self.TableExistsTestCase(
                TableIdent(table_name="//home/yandexbi/datalens-back/bi_test_data/unk", schema_name=None, db_name=None),
                False
            )
        elif param == 'exists':
            yield self.TableExistsTestCase(
                TableIdent(table_name=TEST_TABLE, schema_name=None, db_name=None),
                True
            )
        else:
            raise ValueError(f"Unknown fixture param {param}")

    @pytest.mark.skip("Not relevant for CHYT")
    @pytest.mark.asyncio
    async def test_unauthorized_proxy_access_attempt(self, exec_mode, conn_dto, executor_builder):
        return await super().test_unauthorized_proxy_access_attempt(exec_mode, conn_dto, executor_builder)

    @pytest.mark.skip("Not relevant for CHYT")
    def test_get_table_names_sync(self, sync_exec_wrapper, get_table_names_test_case):
        super().test_get_table_names_sync(sync_exec_wrapper, get_table_names_test_case)

    @pytest.mark.skip("Not relevant for CHYT")
    @pytest.mark.asyncio
    async def test_get_table_names(self, executor, get_table_names_test_case):
        return await super().test_get_table_names(executor, get_table_names_test_case)

    @pytest.mark.asyncio
    async def test_send_trace_context(self, executor_builder):
        """Just for manual check that trace is appears in CHYT Jaeger project"""
        ce = executor_builder.with_options(
            ExecutorOptions(req_ctx_info=attr.evolve(
                RequestContextInfo.create_empty(),
                x_dl_debug_mode=True,
            ))
        ).build()
        await ce.execute(ConnExecutorQuery("SELECT 1"))


@pytest.mark.yt
class BaseTestCHYTAsyncAdapterConnExecutor(BaseCHYTTestSet):
    inf_val = None

    @staticmethod
    def get_log_single_record_by_msg_prefix(caplog, prefix):
        matching_recs = [r for r in caplog.records if r.message.startswith(prefix)]
        assert len(matching_recs) == 1, f"Unexpected count of matched records: {matching_recs}"
        return matching_recs[0]

    @pytest.mark.asyncio
    async def test_no_mirroring(self, executor_builder, default_conn_options, caplog):
        caplog.set_level('DEBUG')
        assert default_conn_options.mirroring_clique_alias is None
        ce = executor_builder.with_options(ExecutorOptions(connect_options=default_conn_options)).build()

        await ce.execute(ConnExecutorQuery("SELECT 1"))
        no_mirroring_log_rec = self.get_log_single_record_by_msg_prefix(caplog, "Mirroring DBA not set")
        assert no_mirroring_log_rec

    @pytest.mark.asyncio
    async def test_mirroring_ok(self, executor_builder, exec_mode, default_conn_options, caplog):
        caplog.set_level('DEBUG')

        mirror_clique = '*chyt_datalens_back'
        mirror_req_timeout = 2.2

        ce = executor_builder.with_options(ExecutorOptions(
            connect_options=attr.evolve(
                default_conn_options,
                mirroring_frac=1.0,
                mirroring_clique_alias=mirror_clique,
                mirroring_clique_req_timeout_sec=mirror_req_timeout,
            ),
            exec_mode=exec_mode,
        )).build()

        # Ok case
        caplog.clear()
        await ce.execute(ConnExecutorQuery("SELECT 1"))
        mirrored_q_result_log_rec = await self.wait_for_log_record(
            caplog=caplog, prefix="Mirrored query result",
            timeout=mirror_req_timeout + 0.5)
        assert (
            mirrored_q_result_log_rec.message ==
            f"Mirrored query result: response from '{mirror_clique}' status=200")

        # Timeout case
        caplog.clear()
        # Hard to make CH sleep for more than 3 seconds.
        query = 'select sleepEachRow(3) from (select arrayJoin(range(1)) as number)'
        await ce.execute(ConnExecutorQuery(query))
        await asyncio.sleep(mirror_req_timeout + 0.5)
        mirrored_q_result_log_rec = await self.wait_for_log_record(
            caplog=caplog, prefix="Mirrored query result",
            timeout=mirror_req_timeout + 0.5)
        assert (
            mirrored_q_result_log_rec.message ==
            "Mirrored query result: timeout")

    @pytest.mark.skip()
    def test_type_discovery_sync(self, sync_exec_wrapper, all_supported_types_test_case):
        pass

    @pytest.mark.skip()
    def test_indexes_discovery(self, executor_builder, exec_mode, default_conn_options, index_test_case, loop):
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


class CHYTTokenAuthMixin:
    @pytest.fixture()
    def conn_dto(self, yt_token) -> ConnDTO:
        return CHYTInternalDTO(
            conn_id=None,
            token=yt_token,
            cluster='hahn',
            clique_alias='*chyt_datalens_back',
        )


class CHYTUserAuthAuthMixin:
    @pytest.fixture()
    def conn_dto(self, yt_token) -> ConnDTO:
        return CHYTUserAuthDTO(
            conn_id=None,
            cluster='hahn',
            clique_alias='*chyt_datalens_back',
            header_authorization='OAuth {}'.format(yt_token),
            header_cookie=None,
        )


class BaseCHYTSyncTestSet(BaseCHYTTestSet):

    @pytest.fixture()
    def select_data_test_set(self):
        return SelectDataTestSet(
            # table=None,
            query=ConnExecutorQuery(
                query=f"""SELECT * FROM "{TEST_TABLE}" ORDER BY count""",
                chunk_size=6,
            ),
            expected_data=TEST_DATA,
        )


@pytest.mark.yt
class TestCHYTSyncAdapterConnExecutor(CHYTTokenAuthMixin, BaseCHYTSyncTestSet):
    connection_type = CONNECTION_TYPE_CH_OVER_YT

    executor_cls = CHYTInternalSyncAdapterConnExecutor


@pytest.mark.yt
class TestCHYTAsyncAdapterConnExecutor(CHYTTokenAuthMixin, BaseTestCHYTAsyncAdapterConnExecutor):
    connection_type = CONNECTION_TYPE_CH_OVER_YT

    executor_cls = CHYTInternalAsyncAdapterConnExecutor


@pytest.mark.yt
class TestCHYTUserAuthSyncAdapterConnExecutor(CHYTUserAuthAuthMixin, BaseCHYTSyncTestSet):
    connection_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH

    executor_cls = CHYTUserAuthSyncAdapterConnExecutor


@pytest.mark.yt
class TestCHYTUserAuthAsyncAdapterConnExecutor(CHYTUserAuthAuthMixin, BaseTestCHYTAsyncAdapterConnExecutor):
    connection_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH

    executor_cls = CHYTUserAuthAsyncAdapterConnExecutor

    @pytest.mark.asyncio
    async def test_unauthorized(self, executor_builder, conn_dto):
        # Making bad request
        broken_conn_dto = attr.evolve(conn_dto,
                                      header_authorization='thereismysecrettoken',
                                      header_cookie='Session_id=secretcookie')
        ce = executor_builder.with_dto(broken_conn_dto).build()
        with pytest.raises(exc.DatabaseUnavailable) as ex:
            await ce.execute(ConnExecutorQuery("SELECT 1"))

        # Check status code
        assert ex.value.orig.code == 401
        # Check hiding secret headers
        assert ex.value.orig.request_info.headers.get('Authorization') == 'the<hidden>ken'
        # Check hiding secret cookies
        assert ex.value.orig.request_info.headers.get('Cookie') == 'Session_id=<hidden>'
        # Check string representation
        assert 'thereismysecrettoken' not in str(ex) and 'secretcookie' not in str(ex)
        assert 'thereismysecrettoken' not in ex.value.message and 'secretcookie' not in ex.value.message

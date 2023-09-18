from __future__ import annotations

import enum

import pytest
import shortuuid
import sqlalchemy as sa
from clickhouse_sqlalchemy import types as ch_types

from dl_constants.enums import BIType, IndexKind

from dl_core import exc
from dl_core.connection_executors import ConnExecutorQuery
from dl_core.connection_models import ConnDTO, TableIdent
from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from dl_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO
from dl_core.db import SchemaInfo, IndexInfo
from dl_core.db.native_type import (
    ClickHouseNativeType, ClickHouseDateTimeWithTZNativeType,
    ClickHouseDateTime64WithTZNativeType,
)
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from bi_legacy_test_bundle_tests.core.common_ce import BaseConnExecutorSet, ErrorTestSet


class CHLikeBaseTestSet(BaseConnExecutorSet):
    inf_val = float('inf')

    def _ch_cd(self, name, sa_type, bi_type, nt_name=None, nullable=True, nt=None, ct=CONNECTION_TYPE_CLICKHOUSE):
        """ clickhouse-specific column definition test-case helper """
        if nt is None:
            assert nt_name
            nt = ClickHouseNativeType(
                conn_type=ct,
                name=nt_name,
                nullable=nullable,
                lowcardinality=False)
        return self.CD(
            cn=name, sa_type=sa_type, ut_bi_type=bi_type,
            nt_name_str=nt_name, nt=nt)

    @pytest.mark.asyncio
    async def test_sa_mod(self, executor):
        result = await executor.execute(ConnExecutorQuery(sa.select([sa.literal(3) % sa.literal(2)])))
        rows = await result.get_all()
        assert rows == [(1,)]

    @pytest.mark.asyncio
    async def test_inf(self, executor):
        result = await executor.execute(ConnExecutorQuery('select 1 / 0'))
        rows = await result.get_all()
        assert rows == [(self.inf_val,)]

    @pytest.mark.asyncio
    async def test_cast_row_to_output(self, executor):
        result = await executor.execute(ConnExecutorQuery('select 1, 1, 1', user_types=[
            BIType.boolean, BIType.float, BIType.integer,
        ]))
        rows = await result.get_all()
        assert rows == [(True, 1.0, 1)]


class BaseClickHouseTestSet(CHLikeBaseTestSet):
    @pytest.fixture()
    def db(self, clickhouse_db):
        return clickhouse_db

    @pytest.fixture()
    def default_conn_options(self) -> CHConnectOptions:
        return CHConnectOptions(
            max_execution_time=100500,
        )

    @pytest.fixture()
    def conn_dto(self, db) -> ConnDTO:
        engine_config = db.engine_config
        assert hasattr(engine_config, 'cluster')
        cluster = engine_config.cluster
        assert cluster is not None
        return ClickHouseConnDTO(
            conn_id=None,
            protocol='http',
            host=db.url.host,
            port=db.url.port,
            endpoint=None,
            cluster_name=cluster,
            db_name=db.url.database,
            username=db.url.username,
            password=db.url.password,
            multihosts=db.get_conn_hosts(),
        )

    @pytest.fixture()
    def error_test_set(self) -> ErrorTestSet:
        return ErrorTestSet(
            query=ConnExecutorQuery("SELECT * FROM table_1234123_not_existing"),
            expected_err_cls=exc.SourceDoesNotExist,
            expected_message_substring="table_1234123_not_existing",
        )

    @pytest.fixture(params=['no_idx', 'one_columns_sorting', 'two_columns_sorting'])
    def index_test_case(self, clickhouse_db, request):
        table_name = f"idx_test_{shortuuid.uuid()}"

        map_case_name_ddl_idx = dict(
            no_idx=(
                "CREATE TABLE `{}` (num_1 UInt64, num_2 UInt64, txt String) ENGINE = Log",
                [],
            ),
            one_columns_sorting=(
                "CREATE TABLE `{}` (num_1 UInt64, num_2 UInt64, txt String)"
                " ENGINE = MergeTree() ORDER BY (num_1)",
                [IndexInfo(
                    columns=('num_1',),
                    kind=IndexKind.table_sorting,
                )]
            ),
            two_columns_sorting=(
                "CREATE TABLE `{}` (num_1 UInt64, num_2 UInt64, txt String)"
                " ENGINE = MergeTree() ORDER BY (num_1, num_2)",
                [IndexInfo(
                    columns=('num_1', 'num_2'),
                    kind=IndexKind.table_sorting,
                )]
            ),
        )

        ddl, expected_indexes = map_case_name_ddl_idx[request.param]

        clickhouse_db.execute(ddl.format(table_name))
        yield self.TypeDiscoveryTestCase(
            table_ident=TableIdent(
                db_name=None,
                schema_name=None,
                table_name=table_name,
            ),
            expected_schema_info=SchemaInfo(
                schema=[],
                indexes=frozenset(expected_indexes)
            ),
        )
        clickhouse_db.execute(f"DROP TABLE IF EXISTS `{table_name}`")

    # TODO FIX: Move table creation here
    @pytest.fixture()
    def all_supported_types_test_case(self, custom_clickhouse_table):
        enum_values = ["allchars '\"\t,= etc", "test", "value1"]
        my_enum8 = enum.Enum('MyEnum8', enum_values)
        my_enum16 = enum.Enum('MyEnum16', enum_values)

        # # Maybe (but will take obscenely long):
        # my_enum16 = enum.Enum('MyEnum16', ['value{}'.format(idx) for idx in range(65530)], start=-32766)
        cd = self._ch_cd

        columns_data = [
            cd('my_int_8', ch_types.Nullable(ch_types.Int8()), BIType.integer, nt_name='int8'),
            cd('my_int_16', ch_types.Nullable(ch_types.Int16()), BIType.integer, nt_name='int16'),
            cd('my_int_32', ch_types.Nullable(ch_types.Int32()), BIType.integer, nt_name='int32'),
            cd('my_int_64', ch_types.Nullable(ch_types.Int64()), BIType.integer, nt_name='int64'),
            cd('my_uint_8', ch_types.Nullable(ch_types.UInt8()), BIType.integer, nt_name='uint8'),
            cd('my_uint_16', ch_types.Nullable(ch_types.UInt16()), BIType.integer, nt_name='uint16'),
            cd('my_uint_32', ch_types.Nullable(ch_types.UInt32()), BIType.integer, nt_name='uint32'),
            cd('my_uint_64', ch_types.Nullable(ch_types.UInt64()), BIType.integer, nt_name='uint64'),
            cd('my_float_32', ch_types.Nullable(ch_types.Float32()), BIType.float, nt_name='float32'),
            cd('my_float_64', ch_types.Nullable(ch_types.Float64()), BIType.float, nt_name='float64'),
            cd('my_string', ch_types.Nullable(ch_types.String()), BIType.string, nt_name='string'),
            # Note: `Nullable(LowCardinality(String))` is actually not allowed:
            # "Nested type LowCardinality(String) cannot be inside Nullable type"
            cd(
                'my_string_lowcardinality',
                ch_types.LowCardinality(ch_types.Nullable(ch_types.String())),
                BIType.string,
                nt=ClickHouseNativeType(
                    conn_type=CONNECTION_TYPE_CLICKHOUSE,
                    name='string',
                    nullable=True,
                    lowcardinality=True),
            ),
            cd(
                'my_string_lowcardinality_nn',
                ch_types.LowCardinality(ch_types.String()),
                BIType.string,
                nt=ClickHouseNativeType(
                    conn_type=CONNECTION_TYPE_CLICKHOUSE,
                    name='string',
                    nullable=False,
                    lowcardinality=True),
            ),
            cd('my_enum8', ch_types.Enum8(my_enum8), BIType.string, nt_name='string', nullable=False),
            cd('my_enum16', ch_types.Enum16(my_enum16), BIType.string, nt_name='string', nullable=False),
            # not nullable so we can check 0000-00-00
            cd('my_date', ch_types.Date(), BIType.date, nt_name='date', nullable=False),
            cd('my_date32', ch_types.Date32(), BIType.date, nt_name='date', nullable=False),
            # not nullable so we can check 0000-00-00 00:00:00
            cd(
                'my_datetime',
                ch_types.DateTime(),
                BIType.genericdatetime,
                nt=ClickHouseDateTimeWithTZNativeType(
                    conn_type=CONNECTION_TYPE_CLICKHOUSE,
                    name='datetimewithtz',
                    nullable=False,
                    lowcardinality=False,
                    timezone_name='UTC',  # the CH system timezone
                    explicit_timezone=False,
                ),
            ),
            cd(
                'my_datetimewithtz',
                ch_types.DateTimeWithTZ('Europe/Moscow'),
                BIType.genericdatetime,
                nt=ClickHouseDateTimeWithTZNativeType(
                    conn_type=CONNECTION_TYPE_CLICKHOUSE,
                    name='datetimewithtz',
                    nullable=False,
                    lowcardinality=False,
                    timezone_name='Europe/Moscow',
                    explicit_timezone=True,
                ),
            ),
            cd(
                'my_datetime64',
                ch_types.DateTime64(6),
                BIType.genericdatetime,
                nt=ClickHouseDateTime64WithTZNativeType(
                    conn_type=CONNECTION_TYPE_CLICKHOUSE,
                    name='datetime64withtz',
                    nullable=False,
                    lowcardinality=False,
                    precision=6,
                    timezone_name='UTC',  # the CH system timezone
                    explicit_timezone=False,
                ),
            ),
            cd(
                'my_datetime64withtz',
                ch_types.DateTime64WithTZ(6, 'Europe/Moscow'),
                BIType.genericdatetime,
                nt=ClickHouseDateTime64WithTZNativeType(
                    conn_type=CONNECTION_TYPE_CLICKHOUSE,
                    name='datetime64withtz',
                    nullable=False,
                    lowcardinality=False,
                    precision=6,
                    timezone_name='Europe/Moscow',
                    explicit_timezone=True,
                ),
            ),
            cd('my_decimal', ch_types.Decimal(8, 4), BIType.float, nt_name='float', nullable=False),
            cd('my_bool', ch_types.Nullable(ch_types.Bool()), BIType.boolean, nt_name='bool'),
        ]

        return self.TypeDiscoveryTestCase(
            table_ident=TableIdent(
                db_name=custom_clickhouse_table.db.name,
                schema_name=None,
                table_name=custom_clickhouse_table.name,
            ),
            expected_schema_info=self.column_data_to_schema_info(
                columns_data,
                CONNECTION_TYPE_CLICKHOUSE
            ),
        )

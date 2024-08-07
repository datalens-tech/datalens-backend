import asyncio
import enum
import os
from typing import (
    Optional,
    Sequence,
)

import attr
from clickhouse_sqlalchemy import types as ch_types
import pytest
import sqlalchemy as sa

from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core.connection_executors import (
    AsyncConnExecutorBase,
    ConnExecutorQuery,
    SyncConnExecutorBase,
)
from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams
from dl_type_transformer.native_type import (
    ClickHouseDateTime64WithTZNativeType,
    ClickHouseDateTimeWithTZNativeType,
    ClickHouseNativeType,
    GenericNativeType,
    norm_native_type,
)

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse_tests.db.config import CoreConnectionSettings
from dl_connector_clickhouse_tests.db.core.base import (
    BaseClickHouseTestClass,
    BaseSslClickHouseTestClass,
)


class ClickHouseSyncAsyncConnectionExecutorCheckBase(
    BaseClickHouseTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionClickhouse],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "Not implemented",
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert "." in db_version


class TestClickHouseSyncConnectionExecutor(
    ClickHouseSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionClickhouse],
):
    @attr.s(frozen=True)
    class CD(DefaultSyncConnectionExecutorTestSuite.CD):
        def get_expected_native_type(self, conn_type: ConnectionType) -> GenericNativeType:
            actual_type = (
                self.sa_type.nested_type
                if isinstance(self.sa_type, (ch_types.Nullable, ch_types.LowCardinality))
                else self.sa_type
            )
            return self.nt or ClickHouseNativeType(
                name=norm_native_type(self.nt_name if self.nt_name is not None else actual_type),
                nullable=isinstance(self.sa_type, ch_types.Nullable),  # note: self.nullable is not taken into account
                lowcardinality=False,
            )

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        enum_values = ["allchars '\"\t,= etc", "test", "value1"]
        tst_enum8 = enum.Enum("TstEnum8", enum_values)
        tst_enum16 = enum.Enum("TstEnum16", enum_values)

        return {
            "ch_types_numbers": [
                self.CD(ch_types.Nullable(ch_types.Int8()), UserDataType.integer),
                self.CD(ch_types.Nullable(ch_types.Int16()), UserDataType.integer),
                self.CD(ch_types.Nullable(ch_types.Int32()), UserDataType.integer),
                self.CD(ch_types.Nullable(ch_types.Int64()), UserDataType.integer),
                self.CD(ch_types.Nullable(ch_types.UInt8()), UserDataType.integer),
                self.CD(ch_types.Nullable(ch_types.UInt16()), UserDataType.integer),
                self.CD(ch_types.Nullable(ch_types.UInt32()), UserDataType.integer),
                self.CD(ch_types.Nullable(ch_types.UInt64()), UserDataType.integer),
                self.CD(ch_types.Nullable(ch_types.Float32()), UserDataType.float),
                self.CD(ch_types.Nullable(ch_types.Float64()), UserDataType.float),
                self.CD(ch_types.Decimal(8, 4), UserDataType.float, nt_name="float"),
            ],
            "ch_types_string": [
                self.CD(sa.String(length=256), UserDataType.string),
                self.CD(ch_types.Nullable(ch_types.String()), UserDataType.string),
                # Note: `Nullable(LowCardinality(String))` is actually not allowed:
                # "Nested type LowCardinality(String) cannot be inside Nullable type"
                self.CD(
                    ch_types.LowCardinality(ch_types.Nullable(ch_types.String())),
                    UserDataType.string,
                    nt=ClickHouseNativeType(
                        name="string",
                        nullable=True,
                        lowcardinality=True,
                    ),
                ),
                self.CD(
                    ch_types.LowCardinality(ch_types.String()),
                    UserDataType.string,
                    nt=ClickHouseNativeType(
                        name="string",
                        nullable=False,
                        lowcardinality=True,
                    ),
                ),
            ],
            "ch_types_date": [
                # not nullable so we can check 0000-00-00
                self.CD(ch_types.Date(), UserDataType.date),
                self.CD(ch_types.Date32(), UserDataType.date, nt_name="date"),
                # not nullable so we can check 0000-00-00 00:00:00
                self.CD(
                    ch_types.DateTime(),
                    UserDataType.genericdatetime,
                    nt=ClickHouseDateTimeWithTZNativeType(
                        name="datetimewithtz",
                        nullable=False,
                        lowcardinality=False,
                        timezone_name="UTC",  # the CH system timezone
                        explicit_timezone=False,
                    ),
                ),
                self.CD(
                    ch_types.DateTimeWithTZ("Europe/Moscow"),
                    UserDataType.genericdatetime,
                    nt=ClickHouseDateTimeWithTZNativeType(
                        name="datetimewithtz",
                        nullable=False,
                        lowcardinality=False,
                        timezone_name="Europe/Moscow",
                        explicit_timezone=True,
                    ),
                ),
                self.CD(
                    ch_types.DateTime64(6),
                    UserDataType.genericdatetime,
                    nt=ClickHouseDateTime64WithTZNativeType(
                        name="datetime64withtz",
                        nullable=False,
                        lowcardinality=False,
                        precision=6,
                        timezone_name="UTC",  # the CH system timezone
                        explicit_timezone=False,
                    ),
                ),
                self.CD(
                    ch_types.DateTime64WithTZ(6, "Europe/Moscow"),
                    UserDataType.genericdatetime,
                    nt=ClickHouseDateTime64WithTZNativeType(
                        name="datetime64withtz",
                        nullable=False,
                        lowcardinality=False,
                        precision=6,
                        timezone_name="Europe/Moscow",
                        explicit_timezone=True,
                    ),
                ),
            ],
            "ch_types_other": [
                self.CD(ch_types.Enum8(tst_enum8), UserDataType.string, nt_name="string"),
                self.CD(ch_types.Enum16(tst_enum16), UserDataType.string, nt_name="string"),
                self.CD(ch_types.Nullable(ch_types.Bool()), UserDataType.boolean),
            ],
        }


class TestClickHouseAsyncConnectionExecutor(
    ClickHouseSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionClickhouse],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_get_db_version: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_test: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )

    @pytest.mark.asyncio
    async def test_sa_mod(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        result = await async_connection_executor.execute(ConnExecutorQuery(sa.select([sa.literal(3) % sa.literal(2)])))
        rows = await result.get_all()
        assert rows == [(1,)]

    @pytest.mark.asyncio
    async def test_inf(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        result = await async_connection_executor.execute(ConnExecutorQuery("select 1 / 0"))
        rows = await result.get_all()
        assert rows == [(None,)]


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslClickHouseSyncConnectionExecutor(
    BaseSslClickHouseTestClass,
    TestClickHouseSyncConnectionExecutor,
):
    def test_test(self, sync_connection_executor: SyncConnExecutorBase) -> None:
        super().test_test(sync_connection_executor)
        self.check_ssl_directory_is_empty()


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslClickHouseAsyncConnectionExecutor(
    BaseSslClickHouseTestClass,
    TestClickHouseAsyncConnectionExecutor,
):
    async def test_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await super().test_test(async_connection_executor)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()

    async def test_multiple_connection_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await super().test_multiple_connection_test(async_connection_executor)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()

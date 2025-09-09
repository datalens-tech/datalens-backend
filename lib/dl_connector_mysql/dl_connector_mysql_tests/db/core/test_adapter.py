import pytest

from dl_core import exc
from dl_core.connection_executors.async_base import DBAdapterQuery
from dl_core.us_entry import RequestContextInfo
from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mysql.core.async_adapters_mysql import AsyncMySQLAdapter
from dl_connector_mysql.core.target_dto import MySQLConnTargetDTO
from dl_connector_mysql_tests.db.core.base import (
    BaseMySQLTestClass,
    BaseRogueMySQLTestClass,
    BaseSslMySQLTestClass,
)


class TestAsyncMySQLAdapter(
    BaseMySQLTestClass,
    BaseAsyncAdapterTestClass[MySQLConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncMySQLAdapter


class TestAsyncRogueMySQLAdapter(
    BaseRogueMySQLTestClass,
    BaseAsyncAdapterTestClass[MySQLConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
            BaseAsyncAdapterTestClass.test_pass_db_query_to_user: "Not relevant",
            BaseAsyncAdapterTestClass.test_timeout: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncMySQLAdapter

    @pytest.mark.asyncio
    async def test_load_local_data(
        self,
        conn_bi_context: RequestContextInfo,
        target_conn_dto: MySQLConnTargetDTO,
    ) -> None:
        dba = self._make_dba(target_conn_dto, conn_bi_context)

        with pytest.raises(exc.SourceProtocolError):
            await dba.execute(DBAdapterQuery(query="select 1"))


class TestAsyncSslMySQLAdapter(
    BaseSslMySQLTestClass,
    BaseAsyncAdapterTestClass[MySQLConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncMySQLAdapter

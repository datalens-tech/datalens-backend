import asyncio
import os
from typing import (
    Optional,
    Sequence,
)
import uuid

import pytest
from sqlalchemy.dialects import postgresql as pg_types

from dl_constants.enums import UserDataType
from dl_core.connection_executors import AsyncConnExecutorBase
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.database import Db
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_sqlalchemy_postgres.base import CITEXT
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql_tests.db.config import CoreConnectionSettings
from dl_connector_postgresql_tests.db.core.base import (
    BasePostgreSQLTestClass,
    BaseSslPostgreSQLTestClass,
)


class PostgreSQLSyncAsyncConnectionExecutorCheckBase(
    BasePostgreSQLTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "",  # TODO: FIXME
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert "." in db_version

    @pytest.fixture(scope="function")
    def enabled_citext_extension(self, db: Db) -> None:
        db.execute("CREATE EXTENSION IF NOT EXISTS CITEXT;")


class TestPostgreSQLSyncConnectionExecutor(
    PostgreSQLSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "",  # TODO: FIXME
        },
    )

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "types_postgres_number": [
                self.CD(pg_types.SMALLINT(), UserDataType.integer),
                self.CD(pg_types.INTEGER(), UserDataType.integer),
                self.CD(pg_types.BIGINT(), UserDataType.integer),
                self.CD(pg_types.REAL(), UserDataType.float),
                self.CD(pg_types.DOUBLE_PRECISION(), UserDataType.float),
                self.CD(pg_types.NUMERIC(), UserDataType.float),
            ],
            "types_postgres_string": [
                self.CD(pg_types.CHAR(), UserDataType.string),
                self.CD(pg_types.VARCHAR(100), UserDataType.string),
                self.CD(pg_types.TEXT(), UserDataType.string),
                self.CD(CITEXT(), UserDataType.string),
            ],
            "types_postgres_date": [
                self.CD(pg_types.DATE(), UserDataType.date),
                self.CD(pg_types.TIMESTAMP(timezone=False), UserDataType.genericdatetime),
                self.CD(pg_types.TIMESTAMP(timezone=True), UserDataType.genericdatetime),
            ],
            "types_postgres_other": [
                self.CD(pg_types.BOOLEAN(), UserDataType.boolean),
                self.CD(pg_types.ENUM("var1", "var2", name=str(uuid.uuid4())), UserDataType.string),
            ],
        }

    def test_type_recognition(
        self,
        request,
        db: Db,
        sync_connection_executor: SyncConnExecutorBase,
        enabled_citext_extension,
    ) -> None:
        super().test_type_recognition(request, db, sync_connection_executor)


class TestPostgreSQLAsyncConnectionExecutor(
    PostgreSQLSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslPostgreSQLSyncConnectionExecutor(
    BaseSslPostgreSQLTestClass,
    TestPostgreSQLSyncConnectionExecutor,
):
    def test_test(self, sync_connection_executor: SyncConnExecutorBase) -> None:
        super().test_test(sync_connection_executor)
        self.check_ssl_directory_is_empty()


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslPostgreSQLAsyncConnectionExecutor(
    BaseSslPostgreSQLTestClass,
    TestPostgreSQLAsyncConnectionExecutor,
):
    async def test_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await super().test_test(async_connection_executor)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()

    async def test_multiple_connection_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await super().test_multiple_connection_test(async_connection_executor)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()

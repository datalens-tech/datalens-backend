from typing import (
    Optional,
    Sequence,
)
import uuid

import pytest
from sqlalchemy.dialects import postgresql as pg_types

from dl_constants.enums import UserDataType
from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.database import Db
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_sqlalchemy_postgres.base import CITEXT
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum.core.us_connection import GreenplumConnection
from dl_connector_greenplum_tests.db.config import DB_NAME
from dl_connector_greenplum_tests.db.core.base import (
    GP6TestClass,
    GP7TestClass,
)


class GreenplumSyncAsyncConnectionExecutorCheckBase(
    DefaultSyncAsyncConnectionExecutorCheckBase[GreenplumConnection],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "",  # TODO: FIXME
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        # Greenplum version format: "PostgreSQL X.Y.Z (Greenplum Database A.B.C ...)"
        assert "Greenplum" in db_version or "." in db_version

    @pytest.fixture(scope="function")
    def enabled_citext_extension(self, db: Db) -> None:
        try:
            db.execute("CREATE EXTENSION IF NOT EXISTS CITEXT;")
        except Exception:
            # CITEXT extension may not be available in all Greenplum versions
            pytest.skip("CITEXT extension not available")


@pytest.mark.usefixtures("enabled_citext_extension")
class TestGP6SyncConnectionExecutor(
    GP6TestClass,
    GreenplumSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[GreenplumConnection],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "",  # TODO: FIXME
        },
    )

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "types_greenplum_number": [
                self.CD(pg_types.SMALLINT(), UserDataType.integer),
                self.CD(pg_types.INTEGER(), UserDataType.integer),
                self.CD(pg_types.BIGINT(), UserDataType.integer),
                self.CD(pg_types.REAL(), UserDataType.float),
                self.CD(pg_types.DOUBLE_PRECISION(), UserDataType.float),
                self.CD(pg_types.NUMERIC(), UserDataType.float),
            ],
            "types_greenplum_string": [
                self.CD(pg_types.CHAR(), UserDataType.string),
                self.CD(pg_types.VARCHAR(100), UserDataType.string),
                self.CD(pg_types.TEXT(), UserDataType.string),
                self.CD(CITEXT(), UserDataType.string),
            ],
            "types_greenplum_date": [
                self.CD(pg_types.DATE(), UserDataType.date),
                self.CD(pg_types.TIMESTAMP(timezone=False), UserDataType.genericdatetime),
                self.CD(pg_types.TIMESTAMP(timezone=True), UserDataType.genericdatetime),
            ],
            "types_greenplum_other": [
                self.CD(pg_types.BOOLEAN(), UserDataType.boolean),
                self.CD(pg_types.ENUM("var1", "var2", name=str(uuid.uuid4())), UserDataType.string),
            ],
        }


class TestGP6AsyncConnectionExecutor(
    GP6TestClass,
    GreenplumSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[GreenplumConnection],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )


@pytest.mark.usefixtures("enabled_citext_extension")
class TestGP7SyncConnectionExecutor(
    GP7TestClass,
    GreenplumSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[GreenplumConnection],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "",  # TODO: FIXME
        },
    )

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "types_greenplum_number": [
                self.CD(pg_types.SMALLINT(), UserDataType.integer),
                self.CD(pg_types.INTEGER(), UserDataType.integer),
                self.CD(pg_types.BIGINT(), UserDataType.integer),
                self.CD(pg_types.REAL(), UserDataType.float),
                self.CD(pg_types.DOUBLE_PRECISION(), UserDataType.float),
                self.CD(pg_types.NUMERIC(), UserDataType.float),
            ],
            "types_greenplum_string": [
                self.CD(pg_types.CHAR(), UserDataType.string),
                self.CD(pg_types.VARCHAR(100), UserDataType.string),
                self.CD(pg_types.TEXT(), UserDataType.string),
                self.CD(CITEXT(), UserDataType.string),
            ],
            "types_greenplum_date": [
                self.CD(pg_types.DATE(), UserDataType.date),
                self.CD(pg_types.TIMESTAMP(timezone=False), UserDataType.genericdatetime),
                self.CD(pg_types.TIMESTAMP(timezone=True), UserDataType.genericdatetime),
            ],
            "types_greenplum_other": [
                self.CD(pg_types.BOOLEAN(), UserDataType.boolean),
                self.CD(pg_types.ENUM("var1", "var2", name=str(uuid.uuid4())), UserDataType.string),
            ],
        }


class TestGP7AsyncConnectionExecutor(
    GP7TestClass,
    GreenplumSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[GreenplumConnection],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )

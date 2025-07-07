from typing import (
    Optional,
    Sequence,
)

import pytest

from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_ydb.core.ydb.us_connection import YDBConnection
from dl_connector_ydb_tests.db.config import CoreConnectionSettings
from dl_connector_ydb_tests.db.core.base import BaseYDBTestClass


class YDBSyncAsyncConnectionExecutorCheckBase(
    BaseYDBTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[YDBConnection],
):
    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        pass


class TestYDBSyncConnectionExecutor(
    YDBSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[YDBConnection],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultSyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultSyncConnectionExecutorTestSuite.test_error_on_select_from_nonexistent_source: "Not implemented",
        },
        mark_tests_skipped={
            DefaultSyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultSyncConnectionExecutorTestSuite.test_type_recognition: "Not implemented",
        },
    )

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        # TODO: Implement test for type recognition
        return {}


class TestYDBAsyncConnectionExecutor(
    YDBSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[YDBConnection],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_error_on_select_from_nonexistent_source: "Not implemented",
        },
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
        },
    )

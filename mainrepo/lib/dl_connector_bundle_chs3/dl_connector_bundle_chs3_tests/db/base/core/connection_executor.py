import abc

import pytest

from dl_connector_bundle_chs3_tests.db.base.core.base import (
    FILE_CONN_TV,
    BaseCHS3TestClass,
)
from dl_core.connection_models import DBIdent
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams


class CHS3SyncAsyncConnectionExecutorTestBase(
    BaseCHS3TestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[FILE_CONN_TV],
    metaclass=abc.ABCMeta,
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_get_db_version: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Assumes it always exists",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        pass


class CHS3SyncConnectionExecutorTestBase(
    CHS3SyncAsyncConnectionExecutorTestBase[FILE_CONN_TV],
    DefaultSyncConnectionExecutorTestSuite[FILE_CONN_TV],
    metaclass=abc.ABCMeta,
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultSyncConnectionExecutorTestSuite.test_type_recognition: "Not implemented",
        },
    )


class CHS3AsyncConnectionExecutorTestBase(
    CHS3SyncAsyncConnectionExecutorTestBase[FILE_CONN_TV],
    DefaultAsyncConnectionExecutorTestSuite[FILE_CONN_TV],
    metaclass=abc.ABCMeta,
):
    pass

from typing import (
    Optional,
    Sequence,
)

import pytest

from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.testcases.connection_executor import (
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)

from dl_connector_trino.core.us_connection import ConnectionTrino
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass


class TrinoSyncConnectionExecutorBase(
    BaseTrinoTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionTrino],
):
    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return None

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert db_version.startswith("0.") or int(db_version) >= 300


class TestTrinoSyncConnectionExecutor(
    TrinoSyncConnectionExecutorBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionTrino],
):
    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            # "trino_types_number": [
            #     self.CD(trino_types.TINYINT(), UserDataType.integer),
            #     self.CD(trino_types.SMALLINT(), UserDataType.integer),
            #     self.CD(trino_types.MEDIUMINT(), UserDataType.integer),
            #     self.CD(trino_types.INTEGER(), UserDataType.integer),
            #     self.CD(trino_types.BIGINT(), UserDataType.integer),
            #     self.CD(trino_types.FLOAT(), UserDataType.float),
            #     self.CD(trino_types.DOUBLE(), UserDataType.float),
            #     self.CD(trino_types.NUMERIC(), UserDataType.float, nt_name="decimal"),
            #     self.CD(trino_types.DECIMAL(), UserDataType.float),
            #     self.CD(trino_types.BIT(1), UserDataType.boolean),
            # ],
            # "trino_types_string": [
            #     self.CD(trino_types.CHAR(), UserDataType.string),
            #     self.CD(trino_types.VARCHAR(100), UserDataType.string),
            #     self.CD(trino_types.TINYTEXT(), UserDataType.string),
            #     self.CD(trino_types.TEXT(), UserDataType.string),
            # ],
            # "trino_types_date": [
            #     self.CD(trino_types.DATE(), UserDataType.date),
            #     self.CD(trino_types.TIMESTAMP(), UserDataType.genericdatetime),
            #     self.CD(trino_types.DATETIME(), UserDataType.genericdatetime),
            # ],
            # "trino_types_other": [
            #     self.CD(trino_types.TINYBLOB(), UserDataType.string),
            #     self.CD(trino_types.BLOB(), UserDataType.string),
            #     self.CD(trino_types.BINARY(), UserDataType.string),
            #     self.CD(trino_types.VARBINARY(100), UserDataType.string),
            #     self.CD(trino_types.ENUM("a", "b", "c", name="some_enum"), UserDataType.string),
            # ],
        }

from typing import Optional

import pytest

from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_models.common_models import (
    DBIdent,
    TableIdent,
)
import dl_core.exc as core_exc
from dl_core_testing.database import Db
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
    def test_error_on_select_from_nonexistent_source(
        self,
        db: Db,
        sync_connection_executor: SyncConnExecutorBase,
        nonexistent_table_ident: TableIdent,
    ) -> None:
        table_ident = ".".join(
            db.quote(s)
            for s in (
                nonexistent_table_ident.db_name,
                nonexistent_table_ident.schema_name,
                nonexistent_table_ident.table_name,
            )
        )
        query = ConnExecutorQuery(query=f"SELECT * from {table_ident}")
        with pytest.raises(core_exc.SourceDoesNotExist):
            sync_connection_executor.execute(query)

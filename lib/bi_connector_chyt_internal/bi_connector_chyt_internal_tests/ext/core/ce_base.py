from typing import (
    Generic,
    Optional,
    Sequence,
    TypeVar,
)

import pytest
import shortuuid
from sqlalchemy.types import TypeEngine

from dl_constants.enums import UserDataType
from dl_core.connection_models.common_models import (
    DBIdent,
    TableIdent,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core_testing.database import DbTable
from dl_core_testing.testcases.connection_executor import DefaultSyncAsyncConnectionExecutorCheckBase


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class CHYTCommonSyncAsyncConnectionExecutorCheckBase(
    DefaultSyncAsyncConnectionExecutorCheckBase[_CONN_TV], Generic[_CONN_TV]
):
    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=None)

    @pytest.fixture(scope="function")
    def db_table(self, sample_table: DbTable) -> DbTable:
        return sample_table

    @pytest.fixture(scope="function")
    def existing_table_ident(self, sample_table: DbTable) -> TableIdent:
        return TableIdent(
            db_name=None,
            schema_name=None,
            table_name=sample_table.name,
        )

    @pytest.fixture(scope="function")
    def nonexistent_table_ident(self, existing_table_ident: TableIdent) -> TableIdent:
        return existing_table_ident.clone(
            table_name=f"//home/yandexbi/datalens-back/nonexistent_table_{shortuuid.uuid()}",
        )

    def check_db_version(self, db_version: Optional[str]) -> None:
        pass

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[tuple[TypeEngine, UserDataType]]]:
        return {}

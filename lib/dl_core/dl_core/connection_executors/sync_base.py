from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Sequence,
)

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connection_executors import ConnExecutorQuery
from dl_core.db import SchemaInfo


if TYPE_CHECKING:
    from dl_core.connection_models.common_models import (
        DBIdent,
        PageIdent,
        SchemaIdent,
        TableDefinition,
        TableIdent,
    )
    from dl_core.connection_models.dto_defs import ConnDTO
    from dl_dashsql.typed_query.primitives import (
        TypedQuery,
        TypedQueryResult,
    )


LOGGER = logging.getLogger(__name__)


@attr.s
class SyncExecutionResult:
    cursor_info: dict = attr.ib()
    result: Iterable[Sequence] = attr.ib()

    def get_all(self) -> Sequence[Sequence]:
        """
        :return: All fetched data
        """
        result: list[Sequence] = []
        for chunk in self.result:
            result.extend(chunk)

        return result


class SyncConnExecutorBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def initialize(self) -> None:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    def execute(self, query: ConnExecutorQuery) -> SyncExecutionResult:
        pass

    @abc.abstractmethod
    def execute_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        pass

    @abc.abstractmethod
    def test(self) -> None:
        pass

    @abc.abstractmethod
    def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        pass

    @abc.abstractmethod
    def get_schema_names(self, db_ident: DBIdent) -> list[str]:
        pass

    @abc.abstractmethod
    def get_tables(self, schema_ident: SchemaIdent, page_ident: PageIdent | None = None) -> list[TableIdent]:
        pass

    @abc.abstractmethod
    def get_table_schema_info(self, table_def: TableDefinition) -> SchemaInfo:
        pass

    @abc.abstractmethod
    def is_table_exists(self, table_ident: TableIdent) -> bool:
        pass

    @abc.abstractmethod
    def is_conn_dto_equals(self, another: ConnDTO) -> bool:
        pass

    @abc.abstractmethod
    def is_context_info_equals(self, another: RequestContextInfo) -> bool:
        pass

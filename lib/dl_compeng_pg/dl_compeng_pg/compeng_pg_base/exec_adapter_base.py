from __future__ import annotations

import abc
import logging
from typing import (
    ClassVar,
    Generic,
    Sequence,
    TypeVar,
    Union,
)

import attr
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.base import Executable

from dl_constants.enums import UserDataType
from dl_core.connectors.base.query_compiler import QueryCompiler
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.streaming import AsyncChunkedBase
from dl_core.db.sa_types import make_sa_type

from dl_connector_postgresql.core.postgresql.constants import BACKEND_TYPE_POSTGRES
from dl_connector_postgresql.core.postgresql_base.type_transformer import PostgreSQLTypeTransformer


LOGGER = logging.getLogger(__name__)

_CONN_TV = TypeVar("_CONN_TV")


@attr.s
class PostgreSQLExecAdapterAsync(Generic[_CONN_TV], ProcessorDbExecAdapterBase, metaclass=abc.ABCMeta):  # noqa
    """
    PG-CompEng-specific adapter.
    Adds DDL functionality and PostgreSQL specificity to the base DB adapter.
    """

    _conn: _CONN_TV = attr.ib(kw_only=True)
    _tt: PostgreSQLTypeTransformer = attr.ib(factory=PostgreSQLTypeTransformer, init=False)

    _log: ClassVar[logging.Logger] = LOGGER.getChild("PostgreSQLExecAdapterAsync")  # type: ignore  # TODO: fix

    @property
    def dialect(self) -> DefaultDialect:
        # Note: not necessarily psycopg2, but should be close enough
        # (especially for debug-compile).
        return PGDialect_psycopg2()

    @abc.abstractmethod
    async def _execute(self, query: Union[str, Executable]) -> None:
        """Execute query without (necessarily) fetching data"""

    async def _execute_ddl(self, query: Union[str, Executable]) -> None:
        """Execute a DDL statement"""
        await self._execute(query)

    def _make_sa_table(self, table_name: str, names: Sequence[str], user_types: Sequence[UserDataType]) -> sa.Table:
        assert len(names) == len(user_types)
        backend_type = BACKEND_TYPE_POSTGRES
        columns = [
            sa.Column(name=name, type_=make_sa_type(
                backend_type=backend_type,
                native_type=self._tt.type_user_to_native(user_t=user_t),
            ))
            for name, user_t in zip(names, user_types)
        ]
        return sa.Table(table_name, sa.MetaData(), *columns, prefixes=["TEMPORARY"])

    async def create_table(  # type: ignore  # 2024-01-29 # TODO: Return type "Coroutine[Any, Any, None]" of "create_table" incompatible with return type "Coroutine[Any, Any, TableClause]" in supertype "ProcessorDbExecAdapterBase"  [override]
        self,
        *,
        table_name: str,
        names: Sequence[str],
        user_types: Sequence[UserDataType],
    ) -> None:
        """Create table in database"""

        table = self._make_sa_table(table_name=table_name, names=names, user_types=user_types)
        self._log.info(f"Creating PG processor table {table_name}: {table}")
        await self._execute_ddl(sa.schema.CreateTable(table))

    async def _drop_table(self, table_name: str) -> None:
        await self._execute_ddl(sa.schema.DropTable(sa.table(table_name)))

    async def drop_table(self, table_name: str) -> None:
        """Drop table in database"""

        self._log.info(f"Dropping PG processor table {table_name}")
        await self._drop_table(table_name=table_name)

    @abc.abstractmethod
    async def insert_data_into_table(
        self,
        *,
        table_name: str,
        names: Sequence[str],
        user_types: Sequence[UserDataType],
        data: AsyncChunkedBase,
    ) -> None:
        """,,,"""

    def get_query_compiler(self) -> QueryCompiler:
        return QueryCompiler(dialect=self.dialect)

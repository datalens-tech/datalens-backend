from __future__ import annotations

import abc
import functools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    List,
    Optional,
    Sequence,
    TypeVar,
)

import attr
from typing_extensions import final

from dl_core.connection_executors.common_base import (
    ConnExecutorBase,
    ConnExecutorQuery,
)
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.db import SchemaInfo


if TYPE_CHECKING:
    from dl_constants.enums import UserDataType
    from dl_constants.types import TBIDataTable
    from dl_core.connection_models.common_models import (
        DBIdent,
        SchemaIdent,
        TableDefinition,
        TableIdent,
    )
    from dl_dashsql.typed_query.primitives import (
        TypedQuery,
        TypedQueryResult,
    )


LOGGER = logging.getLogger(__name__)


@attr.s
class AsyncExecutionResult:
    cursor_info: dict = attr.ib()
    result: AsyncIterable[TBIDataTable] = attr.ib()  # iterable of tables (chunks)
    # for `autodetect_user_types` result
    user_types: Optional[List[UserDataType]] = attr.ib(default=None)
    # DB-specific result. Should be mutable, and get filled after `result` is consumed.
    result_footer: dict = attr.ib(factory=dict)

    async def get_all(self) -> Sequence[Sequence]:
        """
        :return: All fetched data
        """
        result: List[Sequence] = []
        async for chunk in self.result:
            result.extend(chunk)

        return result


_RET_TV = TypeVar("_RET_TV")


def init_required(wrapped: Callable[..., Awaitable[_RET_TV]]) -> Callable[..., Awaitable[_RET_TV]]:
    @functools.wraps(wrapped)
    async def wrapper(self: "AsyncConnExecutorBase", *args: Any, **kwargs: Any) -> _RET_TV:
        if not self._is_initialized:
            await self.initialize()

        return await wrapped(self, *args, **kwargs)

    return wrapper


@attr.s(cmp=False, hash=False)
class AsyncConnExecutorBase(ConnExecutorBase, metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def is_pure_async(cls) -> bool:
        pass

    async def initialize(self) -> None:
        pass

    @abc.abstractmethod
    def executor_query_to_db_adapter_query(self, conn_exec_query: ConnExecutorQuery) -> DBAdapterQuery:
        pass

    # DB Interaction method wrappers
    @final
    @init_required
    async def execute(self, query: ConnExecutorQuery) -> AsyncExecutionResult:
        """
        This method should not be overridden!
        :param query:
        :return: Chunks of result
        """
        return await self._execute(query)

    @final
    @init_required
    async def execute_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        """
        This method should not be overridden!
        :param query:
        :return: Chunks of result
        """
        return await self._execute_typed_query(typed_query)

    @final
    @init_required
    async def test(self) -> None:
        await self._test()

    @final
    @init_required
    async def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return await self._get_db_version(db_ident)

    @final
    @init_required
    async def get_schema_names(self, db_ident: DBIdent) -> List[str]:
        return await self._get_schema_names(db_ident)

    @final
    @init_required
    async def get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        return await self._get_tables(schema_ident)

    @final
    @init_required
    async def get_table_schema_info(self, table_def: TableDefinition) -> SchemaInfo:
        return await self._get_table_schema_info(table_def)

    @final
    @init_required
    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        return await self._is_table_exists(table_ident)

    @final
    async def close(self) -> None:
        LOGGER.info("Method close() of connection executor was called")
        return await self._close()

    # Methods to implements
    @abc.abstractmethod
    async def _execute(self, query: ConnExecutorQuery) -> AsyncExecutionResult:
        pass

    @abc.abstractmethod
    async def _execute_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        pass

    @abc.abstractmethod
    async def _test(self) -> None:
        pass

    @abc.abstractmethod
    async def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        pass

    @abc.abstractmethod
    async def _get_schema_names(self, db_ident: DBIdent) -> List[str]:
        pass

    @abc.abstractmethod
    async def _get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        pass

    @abc.abstractmethod
    async def _get_table_schema_info(self, table_def: TableDefinition) -> SchemaInfo:
        pass

    @abc.abstractmethod
    async def _is_table_exists(self, table_ident: TableIdent) -> bool:
        pass

    @abc.abstractmethod
    async def _close(self) -> None:
        pass

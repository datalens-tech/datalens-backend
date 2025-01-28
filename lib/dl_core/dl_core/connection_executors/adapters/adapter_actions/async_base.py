import abc
from typing import Optional

import attr

from dl_core.connection_executors.models.db_adapter_data import RawSchemaInfo
from dl_core.connection_models import (
    DBIdent,
    SchemaIdent,
    TableDefinition,
    TableIdent,
)
from dl_dashsql.typed_query.primitives import (
    TypedQuery,
    TypedQueryRaw,
    TypedQueryRawResult,
    TypedQueryResult,
)


@attr.s(frozen=True)
class AsyncAdapterAction(abc.ABC):  # noqa: B024
    """
    Base class for all async adapter actions.
    """


# Base classes for specific actions


@attr.s(frozen=True)
class AsyncDBVersionAdapterAction(AsyncAdapterAction):
    @abc.abstractmethod
    async def run_db_version_action(self, db_ident: DBIdent) -> Optional[str]:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncSchemaNamesAdapterAction(AsyncAdapterAction):
    @abc.abstractmethod
    async def run_schema_names_action(self, db_ident: DBIdent) -> list[str]:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTableNamesAdapterAction(AsyncAdapterAction):
    @abc.abstractmethod
    async def run_table_names_action(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTestAdapterAction(AsyncAdapterAction):
    @abc.abstractmethod
    async def run_test_action(self) -> None:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTableInfoAdapterAction(AsyncAdapterAction):
    @abc.abstractmethod
    async def run_table_info_action(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTableExistsAdapterAction(AsyncAdapterAction):
    @abc.abstractmethod
    async def run_table_exists_action(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTypedQueryAdapterAction(AsyncAdapterAction):
    @abc.abstractmethod
    async def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTypedQueryRawAdapterAction(AsyncAdapterAction):
    @abc.abstractmethod
    async def run_typed_query_raw_action(self, typed_query_raw: TypedQueryRaw) -> TypedQueryRawResult:
        raise NotImplementedError


# Dummy "NotImplemented" implementations


@attr.s(frozen=True)
class AsyncDBVersionAdapterActionNotImplemented(AsyncDBVersionAdapterAction):
    async def run_db_version_action(self, db_ident: DBIdent) -> Optional[str]:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncSchemaNamesAdapterActionNotImplemented(AsyncSchemaNamesAdapterAction):
    async def run_schema_names_action(self, db_ident: DBIdent) -> list[str]:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTableNamesAdapterActionNotImplemented(AsyncTableNamesAdapterAction):
    async def run_table_names_action(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError


@attr.s
class AsyncTestAdapterActionNotImplemented(AsyncTestAdapterAction):
    async def run_test_action(self) -> None:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTableInfoAdapterActionNotImplemented(AsyncTableInfoAdapterAction):
    async def run_table_info_action(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTableExistsActionNotImplemented(AsyncTableExistsAdapterAction):
    async def run_table_exists_action(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTypedQueryActionNotImplemented(AsyncTypedQueryAdapterAction):
    async def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        raise NotImplementedError


@attr.s(frozen=True)
class AsyncTypedQueryRawActionNotImplemented(AsyncTypedQueryRawAdapterAction):
    async def run_typed_query_raw_action(self, typed_query_raw: TypedQueryRaw) -> TypedQueryRawResult:
        raise NotImplementedError

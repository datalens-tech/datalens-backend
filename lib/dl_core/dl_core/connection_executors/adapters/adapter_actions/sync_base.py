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
    TypedQueryResult,
)


@attr.s(frozen=True)
class SyncAdapterAction(abc.ABC):  # noqa: B024
    """
    Base class for all adapter actions.
    """


# Base classes for specific actions


@attr.s(frozen=True)
class SyncDBVersionAdapterAction(SyncAdapterAction):
    @abc.abstractmethod
    def run_db_version_action(self, db_ident: DBIdent) -> Optional[str]:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncSchemaNamesAdapterAction(SyncAdapterAction):
    @abc.abstractmethod
    def run_schema_names_action(self, db_ident: DBIdent) -> list[str]:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTableNamesAdapterAction(SyncAdapterAction):
    @abc.abstractmethod
    def run_table_names_action(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTestAdapterAction(SyncAdapterAction):
    @abc.abstractmethod
    def run_test_action(self) -> None:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTableInfoAdapterAction(SyncAdapterAction):
    @abc.abstractmethod
    def run_table_info_action(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTableExistsAdapterAction(SyncAdapterAction):
    @abc.abstractmethod
    def run_table_exists_action(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTypedQueryAdapterAction(SyncAdapterAction):
    @abc.abstractmethod
    def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        raise NotImplementedError


# Dummy "NotImplemented" implementations


@attr.s(frozen=True)
class SyncDBVersionAdapterActionNotImplemented(SyncDBVersionAdapterAction):
    def run_db_version_action(self, db_ident: DBIdent) -> Optional[str]:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncSchemaNamesAdapterActionNotImplemented(SyncSchemaNamesAdapterAction):
    def run_schema_names_action(self, db_ident: DBIdent) -> list[str]:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTableNamesAdapterActionNotImplemented(SyncTableNamesAdapterAction):
    def run_table_names_action(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError


@attr.s
class SyncTestAdapterActionNotImplemented(SyncTestAdapterAction):
    def run_test_action(self) -> None:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTableInfoAdapterActionNotImplemented(SyncTableInfoAdapterAction):
    def run_table_info_action(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTableExistsActionNotImplemented(SyncTableExistsAdapterAction):
    def run_table_exists_action(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError


@attr.s(frozen=True)
class SyncTypedQueryAdapterActionNotImplemented(SyncTypedQueryAdapterAction):
    def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        raise NotImplementedError

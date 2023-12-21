from __future__ import annotations

import abc
from functools import wraps
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Type,
)

import attr
import sqlalchemy as sa
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.elements import ClauseElement

from dl_constants.enums import JoinType
from dl_core import exc
from dl_core.connection_models import (
    DBIdent,
    SATextTableDefinition,
    TableDefinition,
    TableIdent,
)
from dl_core.connectors.base.query_compiler import QueryCompiler
from dl_core.data_source.base import (
    DataSource,
    DbInfo,
    IncompatibleDataSourceMixin,
)
from dl_core.data_source_spec.sql import (
    DbSQLDataSourceSpec,
    IndexedSQLDataSourceSpec,
    SchemaSQLDataSourceSpec,
    SQLDataSourceSpecBase,
    StandardSchemaSQLDataSourceSpec,
    StandardSQLDataSourceSpec,
    SubselectDataSourceSpec,
    TableSQLDataSourceSpec,
)
from dl_core.db import (
    IndexInfo,
    SchemaInfo,
)
from dl_core.utils import sa_plain_text


if TYPE_CHECKING:
    from dl_core.connection_executors.async_base import AsyncConnExecutorBase
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


LOGGER = logging.getLogger(__name__)


def require_table_name(func):  # type: ignore  # TODO: fix
    @wraps(func)
    def wrapper(self, *args, **kwargs):  # type: ignore  # TODO: fix
        if not self.table_name:
            raise exc.TableNameNotConfiguredError

        return func(self, *args, **kwargs)

    return wrapper


@attr.s
class BaseSQLDataSource(DataSource):
    """Data source for SQL database"""

    default_raw_data_batch_size = 10000
    supported_join_types = frozenset(
        {
            JoinType.inner,
            JoinType.left,
        }
    )
    compiler_cls: Type[QueryCompiler] = QueryCompiler

    # Instance attrs
    _exists: Optional[bool] = attr.ib(default=None, init=False, eq=False, hash=False)

    @property
    def spec(self) -> SQLDataSourceSpecBase:
        assert isinstance(self._spec, SQLDataSourceSpecBase)
        return self._spec

    @property
    def db_version(self) -> Optional[str]:
        return self.spec.db_version

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            db_version=self.db_version,
        )

    def get_connect_args(self) -> dict:
        """Customize connection's ``connect_args``"""
        return {}

    def _postprocess_raw_schema_from_db(self, schema_info: SchemaInfo) -> SchemaInfo:
        columns = tuple(col._replace(source_id=self.id) for col in schema_info.schema)

        if not columns:
            raise exc.FailedToLoadSchema()

        return schema_info.clone(schema=columns)

    @property
    def default_title(self) -> str:
        return "SQL"

    def _check_existence(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> bool:
        return True

    async def _check_existence_async(self, conn_executor_factory: Callable[[], AsyncConnExecutorBase]) -> bool:
        return True

    def source_exists(
        self,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
        force_refresh: bool = False,
    ) -> bool:
        if self._exists is None or force_refresh:
            self._exists = self._check_existence(conn_executor_factory=conn_executor_factory)
        return self._exists

    def get_dialect(self) -> DefaultDialect:
        return self.connection.get_dialect()

    def quote(self, value) -> sa.sql.quoted_name:  # type: ignore  # TODO: fix  # subclass of str
        return self.connection.quote(value)

    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        raise NotImplementedError()

    def get_table_definition(self) -> TableDefinition:
        """Should returns TableDefinition which will be used to fetch schema info from DB"""
        raise NotImplementedError()

    def get_schema_info(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> SchemaInfo:
        table_def = self.get_table_definition()
        conn_executor = conn_executor_factory()
        schema_info = conn_executor.get_table_schema_info(table_def)
        return self._postprocess_raw_schema_from_db(schema_info)

    def _get_db_version(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> Optional[str]:
        conn_executor = conn_executor_factory()
        return conn_executor.get_db_version(
            db_ident=DBIdent(db_name=None),
        )

    def get_db_info(
        self,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> DbInfo:
        # self.connection
        LOGGER.info("Fetching version info from the database")
        db_version = self._get_db_version(conn_executor_factory=conn_executor_factory)
        return DbInfo(
            version=db_version,
        )

    def get_cached_db_info(self) -> DbInfo:
        return DbInfo(
            version=self.db_version,
        )


@attr.s
class SubselectDataSource(BaseSQLDataSource):
    """
    `select … from (select …) …` source (for user-input inner-select).
    """

    _subsql: Optional[str] = attr.ib(default=None)

    @property
    def spec(self) -> SubselectDataSourceSpec:
        assert isinstance(self._spec, SubselectDataSourceSpec)
        return self._spec

    @property
    def subsql(self) -> Optional[str]:
        return self.spec.subsql

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            subsql=self.subsql,
        )

    _subquery_alias_joiner = " AS "
    _subquery_auto_alias = "source"

    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        if not self.connection.is_subselect_allowed:
            raise exc.SubselectNotAllowed()

        subsql = self.subsql
        if not subsql:
            raise exc.TableNameNotConfiguredError

        from_sql = "(\n{}\n)".format(subsql)

        # e.g. PG doesn't allow unaliased subqueries at all.
        if alias is None:
            alias = self._subquery_auto_alias

        if alias is not None:
            from_sql = "{}{}{}".format(from_sql, self._subquery_alias_joiner, self.quote(alias))
        return sa_plain_text(from_sql)

    @property
    def default_title(self) -> str:
        return "(select ...)"

    def get_table_definition(self) -> TableDefinition:
        subselect = self.get_sql_source(alias="source")
        return SATextTableDefinition(text=subselect)


@attr.s
class SQLDataSource(abc.ABC, BaseSQLDataSource):
    """Data source for SQL database"""

    @property
    @abc.abstractmethod
    def db_name(self) -> Optional[str]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def table_name(self) -> Optional[str]:
        raise NotImplementedError

    @property
    def default_title(self) -> str:
        return self.table_name  # type: ignore  # TODO: fix

    def _get_db_version(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> Optional[str]:
        conn_executor = conn_executor_factory()
        return conn_executor.get_db_version(
            db_ident=DBIdent(db_name=self.db_name),
        )

    def _check_existence(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> bool:
        table_def = self.get_table_definition()
        conn_executor = conn_executor_factory()
        return conn_executor.is_table_exists(table_def)

    async def _check_existence_async(self, conn_executor_factory: Callable[[], AsyncConnExecutorBase]) -> bool:
        table_def = self.get_table_definition()
        async_conn_executor = conn_executor_factory()
        return await async_conn_executor.is_table_exists(table_def)

    def source_exists(
        self,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
        force_refresh: bool = False,
    ) -> bool:
        if not self.table_name:
            return False

        return super().source_exists(conn_executor_factory=conn_executor_factory, force_refresh=force_refresh)

    @require_table_name
    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        q = self.quote
        alias_str = "" if alias is None else f" AS {q(alias)}"
        return sa_plain_text(f"{q(self.db_name)}.{q(self.table_name)}{alias_str}")

    @require_table_name
    def get_table_definition(self) -> TableDefinition:
        table_name = self.table_name
        assert table_name is not None
        return TableIdent(db_name=self.db_name, schema_name=None, table_name=table_name)

    @property
    def is_configured(self) -> bool:
        return self.db_name is not None and self.table_name is not None


@attr.s
class DbSQLDataSourceMixin(BaseSQLDataSource):
    """SQL data source with db_name"""

    @property
    def db_name(self) -> Optional[str]:
        assert isinstance(self._spec, DbSQLDataSourceSpec)
        return self._spec.db_name or getattr(self.connection, "db_name", None)


@attr.s
class TableSQLDataSourceMixin(BaseSQLDataSource):
    """SQL data source with table_name"""

    @property
    def table_name(self) -> Optional[str]:
        assert isinstance(self._spec, TableSQLDataSourceSpec)
        return self._spec.table_name or getattr(self.connection, "table_name", None)

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            table_name=self.table_name,
        )


@attr.s
class IndexedSQLDataSourceMixin(BaseSQLDataSource):
    """SQL data source with index info"""

    @property
    def saved_index_info_set(self) -> Optional[frozenset[IndexInfo]]:
        assert isinstance(self._spec, IndexedSQLDataSourceSpec)
        return self._spec.index_info_set


@attr.s
class StandardSQLDataSource(
    DbSQLDataSourceMixin,
    TableSQLDataSourceMixin,
    IndexedSQLDataSourceMixin,
    SQLDataSource,  # <-- the main logic is in this one
):
    """SQL data source with db_name, table_name and indexes"""

    @property
    def spec(self) -> StandardSQLDataSourceSpec:
        assert isinstance(self._spec, StandardSQLDataSourceSpec)
        return self._spec

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            db_name=self.spec.db_name,  # FIXME: Remove (db_name is part of the data source only in CH)
            table_name=self.spec.table_name,
        )


class PseudoSQLDataSource(IncompatibleDataSourceMixin, StandardSQLDataSource):
    supported_join_types: ClassVar[frozenset[JoinType]] = frozenset()
    supports_schema_update: ClassVar[bool] = False

    @require_table_name
    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        # ignore alias
        return sa.table(self.table_name)


class SchemaSQLDataSourceMixin(BaseSQLDataSource):
    @property
    def schema_name(self) -> Optional[str]:
        assert isinstance(self._spec, SchemaSQLDataSourceSpec)
        # TODO FIX: DO NOT DO THIS IN PROPERTY!!!!!
        #  USE METHOD get_effective schema name or dump from connection on initial data source creation
        return self._spec.schema_name if self._spec.schema_name else self.get_connection_cls().default_schema_name  # type: ignore  # TODO: fix


@attr.s
class StandardSchemaSQLDataSource(StandardSQLDataSource, SchemaSQLDataSourceMixin):
    _schema_name: Optional[str] = attr.ib(default=None)

    @property
    def spec(self) -> StandardSchemaSQLDataSourceSpec:
        assert isinstance(self._spec, StandardSchemaSQLDataSourceSpec)
        return self._spec

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            schema_name=self.schema_name,
        )

    @require_table_name
    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        if not self.schema_name:
            return super().get_sql_source(alias=alias)
        q = self.quote
        alias_str = "" if alias is None else f" AS {q(alias)}"
        schema_str = "" if self.schema_name is None else f"{q(self.schema_name)}."
        return sa_plain_text(f"{q(self.db_name)}.{schema_str}{q(self.table_name)}{alias_str}")

    @require_table_name
    def get_table_definition(self) -> TableDefinition:
        table_name = self.table_name
        assert table_name is not None
        return TableIdent(db_name=self.db_name, schema_name=self.schema_name, table_name=table_name)

from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Iterable,
    NamedTuple,
)

import attr
from sqlalchemy.sql import TableClause

from dl_cache_engine.primitives import LocalKeyRepresentation
from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    JoinType,
)
from dl_constants.exc import DLBaseException
from dl_core.base_models import (
    ConnectionRef,
    SourceFilterSpec,
)
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connectors.base.query_compiler import QueryCompiler
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.db import (
    IndexInfo,
    SchemaColumn,
    SchemaInfo,
)
from dl_core.exc import (
    ConnectionTemplateDisabledError,
    TemplateInvalidError,
)
from dl_core.query.bi_query import SqlSourceType
from dl_core.us_connection import get_connection_class
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_model_tools.typed_values import BIValue
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    get_type_transformer,
)


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class DbInfo(NamedTuple):
    version: str | None


@attr.s()
class DataSourceRenderer:
    values: dict[str, BIValue] = attr.ib()

    def _render_bi_value(self, value: BIValue) -> str:
        return str(value.value)

    def _render(self, value: str) -> str:
        value = value.strip()

        if value in self.values:
            return self._render_bi_value(self.values[value])

        raise TemplateInvalidError(f"Key {value} not found in {self.values}")

    def render(self, value: str) -> str:
        result = []

        i = 0
        block_start = -1

        while i < len(value):
            if i + 1 < len(value) and value[i : i + 2] == "{{":
                if block_start != -1:
                    # We are already in a block, add it as is including opening brackets, shifting the block start
                    result.append(value[block_start - 2 : i])
                    block_start = -1

                i += 2
                while i < len(value) and value[i] == "{":
                    i += 1
                    result.append("{")

                block_start = i
            elif i + 1 < len(value) and value[i : i + 2] == "}}":
                if block_start == -1:
                    result.append(value[i : i + 2])
                else:
                    block = value[block_start:i]
                    result.append(self._render(block))
                    block_start = -1
                i += 2
            else:
                if block_start == -1:
                    result.append(value[i])
                i += 1

        # If we are still in a block, add it as is including opening brackets
        if block_start != -1:
            result.append(value[block_start - 2 :])

        return "".join(result)


@attr.s()
class DataSource(metaclass=abc.ABCMeta):
    """
    Abstraction layer for all data sources:
    - various kinds of DBs (SQL and non-SQL),
    - CSV,
    - HTTP API-based data sources,
    etc.
    """

    supported_join_types: ClassVar[frozenset[JoinType]] = frozenset()
    supports_schema_update: ClassVar[bool] = True
    supports_sample_usage: ClassVar[bool] = False
    supports_limit: ClassVar[bool] = True
    supports_offset: ClassVar[bool] = True
    supports_preview_from_subquery: ClassVar[bool] = True
    store_raw_schema: ClassVar[bool] = True
    preview_enabled: ClassVar[bool] = True
    default_chunk_row_count: ClassVar[int] = 10000
    chunk_size_bytes: ClassVar[int] = 3 * 1024**2

    compiler_cls: type[QueryCompiler] = QueryCompiler
    conn_type: ClassVar[ConnectionType]  # TODO unbind DataSource and Connection classes BI-4083

    # TODO FIX: Remove ASAP
    _id: str = attr.ib(default=None)
    _us_entry_buffer: USEntryBuffer = attr.ib(default=None, eq=False)
    _spec: DataSourceSpec | None = attr.ib(kw_only=True, default=None)
    _dataset_parameter_values: dict[str, BIValue] = attr.ib(kw_only=True)
    _dataset_template_enabled: bool = attr.ib(kw_only=True)

    _connection: ConnectionBase | None = attr.ib(default=None)

    _cache_enabled: bool = attr.ib(default=True)

    @property
    def spec(self) -> DataSourceSpec:
        assert self._spec is not None
        return self._spec

    def _validate_connection(self) -> None:
        if self._connection is not None and self.spec.connection_ref is None:
            # TODO CONSIDER: extraction of connection ref
            pass
        elif self.spec.connection_ref is not None and self._connection is None:
            pass
        else:
            raise ValueError(
                f"Unexpected combination of 'connection' and 'connection_ref':"
                f" {self._connection} and {self.spec.connection_ref}"
            )

        if self._connection is not None:
            self._validate_connection_cls(self._connection)

    def __attrs_post_init__(self) -> None:
        self._validate_connection()

    @classmethod
    def get_connection_cls(cls) -> type[ConnectionBase]:
        """
        Method should return connection class for this data source.
        Class var not used to do not import connection on module level.
        TODO CONSIDER: May be data sources should directly import connections???
        """
        assert cls.conn_type is not None
        return get_connection_class(cls.conn_type)

    def _validate_connection_cls(self, connection: ConnectionBase) -> None:
        expected_cls = self.get_connection_cls()

        if expected_cls != type(connection):
            raise TypeError(f"Unexpected connection class {type(connection)} for {type(self)}, expected {expected_cls}")

    def initialize(self) -> None:
        """All IO-dependent initialization goes here (whatever can later be made awaitable)"""
        return

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        raise NotImplementedError

    def get_parameters(self) -> dict:
        return {}

    @property
    def id(self) -> str | None:
        return self._id

    @property
    def connection(self) -> ConnectionBase:
        return self._get_connection()

    def _get_connection(self) -> ConnectionBase:
        if self._connection is None:
            connection_ref = self.spec.connection_ref
            assert connection_ref is not None
            loaded_conn = self._us_entry_buffer.get_entry(connection_ref)
            assert isinstance(loaded_conn, ConnectionBase)
            self._validate_connection_cls(loaded_conn)
            self._connection = loaded_conn

        return self._connection

    def get_connection_attr(self, attr_name: str, strict: bool = False) -> Any:
        try:
            if strict:
                return getattr(self.connection, attr_name)
            else:
                return getattr(self.connection, attr_name, None)
        except DLBaseException as e:
            if strict:
                raise
            LOGGER.warning(f"Error while getting {attr_name} from connection: {str(e)}")
            return None

    @property
    def connection_ref(self) -> ConnectionRef:
        if self.spec.connection_ref is None:
            # TODO CONSIDER: May raise exception if connection has no ref? Or raise on save attempt?
            assert self._connection is not None
            self.spec.connection_ref = self._connection.conn_ref

        connection_ref = self.spec.connection_ref
        assert connection_ref is not None
        return connection_ref

    @property
    def data_dump_id(self) -> str | None:
        return self.spec.data_dump_id

    @property
    def cache_enabled(self) -> bool:
        conn = self._connection
        if conn is not None:
            if not conn.allow_cache:
                return False
        return self._cache_enabled

    def get_schema_info(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> SchemaInfo:
        """Return structure of the data source. Implementation-specific."""
        raise NotImplementedError()

    @property
    def saved_raw_schema(self) -> list[SchemaColumn] | None:
        if self.spec.raw_schema is not None:
            return self.spec.raw_schema
        return None

    @property
    def saved_index_info_set(self) -> frozenset[IndexInfo] | None:
        return None

    @abc.abstractmethod
    def get_sql_source(self, alias: str | None = None) -> SqlSourceType:
        """
        Return something that can be used in a ``select_from`` ``SQLAlchemy`` clause for fetching data.
        Optionally assign the source an alias that can be used in the query to refer to it.
        """
        raise NotImplementedError()

    def _render_dataset_parameter_values(
        self,
        value: str,
    ) -> str:
        if not self._dataset_template_enabled:
            return value

        connection = self._get_connection()
        if not connection.is_datasource_template_allowed:
            raise ConnectionTemplateDisabledError

        renderer = DataSourceRenderer(values=self._dataset_parameter_values)

        return renderer.render(value)

    def is_templated(self) -> bool:
        return False

    def _is_value_templated(self, value: str | None) -> bool:
        if value is None:
            return False

        try:
            return self._render_dataset_parameter_values(value) != value
        except ConnectionTemplateDisabledError:
            return False
        except TemplateInvalidError:
            return True

    def source_exists(
        self,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
        force_refresh: bool = False,
    ) -> bool:
        return True

    def get_db_info(
        self,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> DbInfo:
        return self.get_cached_db_info()

    def get_cached_db_info(self) -> DbInfo:
        return DbInfo(
            version=None,
        )

    @property
    def type_transformer(self) -> TypeTransformer:
        return get_type_transformer(self.conn_type)

    def get_expression_value_range(self, col_name: str) -> tuple[Any, Any]:
        """Return MIN and MAX values for given expression"""
        return None, None

    def supports_join_type(self, join_type: JoinType) -> bool:
        return join_type in self.supported_join_types

    @property
    def default_title(self) -> str:
        raise NotImplementedError

    @property
    def is_configured(self) -> bool:
        return True

    def get_filters(self, service_registry: ServicesRegistry) -> Iterable[SourceFilterSpec]:
        return []

    @property
    def data_export_forbidden(self) -> bool:
        return self.connection.data_export_forbidden

    def get_cache_key_part(self) -> LocalKeyRepresentation:
        local_key_rep = self.connection.get_cache_key_part()

        sql_source = self.get_sql_source()
        if isinstance(sql_source, TableClause):
            compiled_sql_source = str(sql_source)
        else:
            compiled_sql_source = sql_source.compile(compile_kwargs={"literal_binds": True}).string

        local_key_rep = local_key_rep.extend(
            part_type="data_source_sql",
            part_content=compiled_sql_source,
        )
        return local_key_rep


class IncompatibleDataSourceMixin(DataSource):
    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return False

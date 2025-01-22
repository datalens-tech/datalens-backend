from __future__ import annotations

import abc
import contextlib
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import attr
import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.engine.base import Engine
from typing_extensions import final

from dl_app_tools.profiling_base import (
    GenericProfiler,
    generic_profiler,
)
from dl_core import exc
from dl_core.connection_executors.adapters.adapters_base import SyncDirectDBAdapter
from dl_core.connection_executors.adapters.mixins import (
    SAColumnTypeNormalizer,
    WithCursorInfo,
    WithDatabaseNameOverride,
    WithNoneRowConverters,
)
from dl_core.connection_executors.adapters.sa_utils import (
    CursorLogger,
    compile_query_for_debug,
    get_db_version_query,
)
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
    RawColumnInfo,
    RawIndexInfo,
    RawSchemaInfo,
)
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connection_models import (
    DBIdent,
    SATextTableDefinition,
    SchemaIdent,
    TableDefinition,
    TableIdent,
)
from dl_core.connectors.base.error_handling import ETBasedExceptionMaker
from dl_core.connectors.base.error_transformer import (
    DbErrorTransformer,
    make_default_transformer_with_custom_rules,
)
from dl_core.db_session_utils import db_session_context
from dl_type_transformer.native_type import CommonNativeType
from dl_utils.utils import get_type_full_name


if TYPE_CHECKING:
    from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO  # noqa: F401


LOGGER = logging.getLogger(__name__)

_DBA_SA_TV = TypeVar("_DBA_SA_TV", bound="BaseSAAdapter")
_DBA_SA_DTO_TV = TypeVar("_DBA_SA_DTO_TV", bound="ConnTargetDTO")


@attr.s(frozen=True, auto_attribs=True)
class EventListenerSpec:
    event_name: str
    callback: Callable


@attr.s()
class BaseSAAdapter(
    WithCursorInfo,
    WithDatabaseNameOverride,
    WithNoneRowConverters,
    ETBasedExceptionMaker,
    SyncDirectDBAdapter[_DBA_SA_DTO_TV],
    SAColumnTypeNormalizer,
):
    allow_sa_text_as_columns_source: ClassVar[bool] = False

    # Instance attributes
    _default_chunk_size: int = attr.ib()
    _target_dto: _DBA_SA_DTO_TV = attr.ib()
    _req_ctx_info: DBAdapterScopedRCI = attr.ib()
    # Internals
    _engine_cache: Dict[Tuple[Optional[str], bool], Engine] = attr.ib(init=False, factory=lambda: {})
    _error_transformer: ClassVar[DbErrorTransformer] = make_default_transformer_with_custom_rules()

    _COMMON_ENGINE_EVENT_LISTENERS: ClassVar[list[EventListenerSpec]] = [
        EventListenerSpec("before_cursor_execute", CursorLogger.before_cursor_execute_handler),
        EventListenerSpec("after_cursor_execute", CursorLogger.after_cursor_execute_handler),
    ]

    @classmethod
    def create(
        cls: Type[_DBA_SA_TV], target_dto: _DBA_SA_DTO_TV, req_ctx_info: DBAdapterScopedRCI, default_chunk_size: int
    ) -> _DBA_SA_TV:
        return cls(target_dto=target_dto, default_chunk_size=default_chunk_size, req_ctx_info=req_ctx_info)

    @property
    def conn_id(self) -> Optional[str]:
        return self._target_dto.conn_id

    @property
    def default_chunk_size(self) -> int:
        return self._default_chunk_size

    def get_target_host(self) -> Optional[str]:
        return self._target_dto.get_effective_host()

    def get_extra_engine_event_listeners(self) -> list[EventListenerSpec]:
        return []

    @final
    def get_db_engine(self, db_name: Optional[str], disable_streaming: bool = False) -> Engine:
        effective_db_name = self.get_db_name_for_query(db_name_from_query=db_name)

        cache_key: Tuple[Optional[str], bool] = effective_db_name, disable_streaming

        if cache_key not in self._engine_cache:
            new_engine = self._get_db_engine(db_name=effective_db_name, disable_streaming=disable_streaming)
            # Adding event listeners
            for listener in self._COMMON_ENGINE_EVENT_LISTENERS:
                event.listen(new_engine, listener.event_name, listener.callback)
            for listener in self.get_extra_engine_event_listeners():
                event.listen(new_engine, listener.event_name, listener.callback)

            self._engine_cache[cache_key] = new_engine

        return self._engine_cache[cache_key]

    @abc.abstractmethod
    def _get_db_engine(self, db_name: str, disable_streaming: bool = False) -> Engine:
        pass

    @contextlib.contextmanager
    def execution_context(self) -> Generator[None, None, None]:
        yield

    @generic_profiler("db-full")
    def execute_by_steps(self, db_adapter_query: DBAdapterQuery) -> Generator[ExecutionStep, None, None]:
        """Generator that yielding messages with data chunks and execution meta-info"""
        chunk_size = db_adapter_query.get_effective_chunk_size(self.default_chunk_size)
        query = db_adapter_query.query

        engine = self.get_db_engine(
            db_name=db_adapter_query.db_name,
            disable_streaming=db_adapter_query.disable_streaming,
        )

        # TODO FIX: Delegate query compilation for debug to error handler or make method of debug compilation
        compiled_query = query if isinstance(query, str) else compile_query_for_debug(query, engine.dialect)

        with (
            db_session_context(backend_type=self.get_backend_type(), db_engine=engine) as db_session,
            self.handle_execution_error(compiled_query),
            self.execution_context(),
        ):
            with GenericProfiler("db-exec"):
                result = db_session.execute(
                    query,
                    # *args,
                    # **kwargs,
                )

            cursor_info = ExecutionStepCursorInfo(
                cursor_info=self._make_cursor_info(result.cursor, db_session=db_session),  # type: ignore  # 2024-01-24 # TODO: "Result" has no attribute "cursor"  [attr-defined]
                raw_cursor_description=list(result.cursor.description),  # type: ignore  # 2024-01-24 # TODO: "Result" has no attribute "cursor"  [attr-defined]
                raw_engine=engine,
            )
            yield cursor_info

            row_converters = self._get_row_converters(cursor_info=cursor_info)
            while True:
                LOGGER.info("Fetching %s rows (conn %s)", chunk_size, self.conn_id)
                with GenericProfiler("db-fetch"):
                    rows = result.fetchmany(chunk_size)

                if not rows:
                    LOGGER.info("No rows remaining")
                    break

                LOGGER.info("Rows fetched, yielding")
                yield ExecutionStepDataChunk(
                    tuple(
                        tuple(
                            (col_converter(val) if col_converter is not None and val is not None else val)
                            # for val, col_converter in zip_longest(row, row_converters)
                            for val, col_converter in zip(row, row_converters, strict=True)
                        )
                        for row in rows
                    )
                )

    @final
    def test(self) -> None:
        with self.handle_execution_error(debug_compiled_query="<test()>"):
            self._test()

    @final
    def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        with self.handle_execution_error(debug_compiled_query=f"<get_db_version({db_ident})>"):
            return self._get_db_version(db_ident)

    @final
    def get_schema_names(self, db_ident: DBIdent) -> List[str]:
        with (
            self.handle_execution_error(debug_compiled_query=f"<get_schema_names({db_ident})>"),
            self.execution_context(),
        ):
            return self._get_schema_names(db_ident)

    @final
    def get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        with (
            self.handle_execution_error(debug_compiled_query=f"<get_tables({schema_ident})>"),
            self.execution_context(),
        ):
            return self._get_tables(schema_ident)

    @final
    def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        debug_compiled_query: str
        exc_post_processor: Optional[Callable[[exc.DatabaseQueryError], None]] = None

        if isinstance(table_def, TableIdent):
            debug_compiled_query = f"<get_columns({table_def})>"

        elif isinstance(table_def, SATextTableDefinition):
            debug_compiled_query = "<get_columns(<subselect>)>"

            # TODO FIX: Do not fill details at such low level
            #  Better way is to set flag that this was a user subselect and depending on user permissions
            #  fill details at bi-api level
            # A catch-point intended for adding `query` and `db_message` only for subselect-schema errors.
            def exc_post_processor(db_exc: exc.DatabaseQueryError) -> None:
                assert isinstance(db_exc, exc.DatabaseQueryError)
                db_exc.query = "SELECT * FROM {}".format(table_def.text)
                db_exc.details.setdefault("query", db_exc.query)
                db_exc.details.setdefault("db_message", db_exc.db_message)

        else:
            raise TypeError(
                f"Unsupported type of table table definition to get info: {get_type_full_name(type(table_def))}"
            )

        with (
            self.handle_execution_error(
                debug_compiled_query=debug_compiled_query,
                exc_post_processor=exc_post_processor,
            ),
            self.execution_context(),
        ):
            if isinstance(table_def, TableIdent):
                columns = self._get_raw_columns_info(table_def)
                indexes: Optional[Tuple[RawIndexInfo, ...]] = None

                if fetch_idx_info and isinstance(table_def, TableIdent):
                    try:
                        with GenericProfiler("db-idx-fetch"):
                            indexes = self._get_table_indexes(table_def)

                    except Exception:  # noqa
                        LOGGER.exception("Indexes fetching fail for %s", table_def)

                return RawSchemaInfo(columns=columns, indexes=indexes)

            elif isinstance(table_def, SATextTableDefinition):
                return self._get_subselect_table_info(table_def)

            else:
                raise TypeError(
                    f"Unsupported type of table table definition to get info: {get_type_full_name(type(table_def))}"
                )

        raise AssertionError("Non handled case of get_table_info()")

    @final
    def is_table_exists(self, table_ident: TableIdent) -> bool:
        with self.handle_execution_error(f"<exists({table_ident})>"), self.execution_context():
            return self._is_table_exists(table_ident)

    def _test(self) -> None:
        self.execute(DBAdapterQuery("select 1")).get_all()

    def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        """Return database version string for this connection"""
        return self.execute(get_db_version_query(db_ident)).get_all()[0][0]

    def _get_schema_names(self, db_ident: DBIdent) -> List[str]:
        db_engine = self.get_db_engine(db_ident.db_name)
        return sa.inspect(db_engine).get_schema_names()

    def _get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        db_name = schema_ident.db_name
        schema_name = schema_ident.schema_name

        db_engine = self.get_db_engine(db_name)
        table_list = sa.inspect(db_engine).get_table_names(schema=schema_name)
        view_list = sa.inspect(db_engine).get_view_names(schema=schema_name)
        return [
            TableIdent(
                db_name=db_name,
                schema_name=schema_name,
                table_name=name,
            )
            for name in table_list + view_list
        ]

    def _get_sa_table_columns(self, table_def: TableDefinition) -> List[Dict[str, Any]]:
        db_engine: sa.engine.Engine
        columns_source: Union[str, sa.sql.elements.TextClause]
        if isinstance(table_def, TableIdent):
            columns_source = table_def.table_name
            db_schema = table_def.schema_name
            db_engine = self.get_db_engine(table_def.db_name)
        elif isinstance(table_def, SATextTableDefinition) and self.allow_sa_text_as_columns_source:
            columns_source = table_def.text
            db_schema = None
            db_engine = self.get_db_engine(None)
        else:
            raise AssertionError(
                f"{get_type_full_name(type(self))}._get_sa_table_columns() does not support"
                f" table_def {get_type_full_name(type(table_def))}"
            )

        inspection = sa.inspect(db_engine)
        table_columns = inspection.get_columns(columns_source, schema=db_schema)
        table_columns = list(table_columns)
        return table_columns

    def _get_raw_columns_info(self, table_def: TableDefinition) -> Tuple[RawColumnInfo, ...]:
        table_columns = self._get_sa_table_columns(table_def)

        return tuple(
            RawColumnInfo(
                name=column["name"],
                title=column.get("title"),
                nullable=column.get("nullable", True),  # To be deprecated.
                native_type=CommonNativeType.normalize_name_and_create(
                    name=self.normalize_sa_col_type(column["type"]),
                    nullable=column.get("nullable", True),
                ),
            )
            for column in table_columns
        )

    def _get_table_indexes(self, table_ident: TableIdent) -> Tuple[RawIndexInfo, ...]:
        db_engine = self.get_db_engine(table_ident.db_name)
        inspection = sa.inspect(db_engine)

        idx_list: List[Dict[str, Any]] = inspection.get_indexes(table_ident.table_name, schema=table_ident.schema_name)

        return tuple(
            RawIndexInfo(
                columns=tuple(idx["column_names"]),
                unique=idx["unique"],
                kind=None,
            )
            for idx in idx_list
        )

    def _get_subselect_table_info(self, subquery: SATextTableDefinition) -> RawSchemaInfo:
        raise Exception(f"`_get_subselect_table_info` is not implemented on SA adapter {self!r}")

    def _is_table_exists(self, table_ident: TableIdent) -> bool:
        eng = self.get_db_engine(table_ident.db_name)
        return sa.inspect(eng).has_table(table_ident.table_name, schema=table_ident.schema_name)

    def close(self) -> None:
        LOGGER.info("Method .close() of SA DB adapter was called")
        # TODO FIX: Find all created generators and close it
        for eng in self._engine_cache.values():
            eng.dispose()

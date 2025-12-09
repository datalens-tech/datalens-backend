from __future__ import annotations

import logging
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generator,
    Optional,
    TypeVar,
)

import attr
import sqlalchemy as sa
import ydb_sqlalchemy.sqlalchemy as ydb_sa

from dl_app_tools.profiling_base import (
    GenericProfiler,
    generic_profiler,
)
from dl_core import exc
from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
)
from dl_core.connection_models import TableIdent
from dl_core.db_session_utils import db_session_context
import dl_sqlalchemy_ydb.dialect

import dl_connector_ydb.core.base.row_converters


if TYPE_CHECKING:
    from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO  # noqa: F401
    from dl_core.connection_executors.models.db_adapter_data import ExecutionStepCursorInfo
    from dl_core.connection_models import DBIdent
    from dl_core.connectors.base.error_transformer import DBExcKWArgs
    from dl_type_transformer.native_type import SATypeSpec


LOGGER = logging.getLogger(__name__)

_DBA_YQL_BASE_DTO_TV = TypeVar("_DBA_YQL_BASE_DTO_TV", bound="BaseSQLConnTargetDTO")


def extract_sleep_duration(text: str) -> float | None:
    """
    Checks if a string looks like '--SLEEP(duration_seconds)' and extracts the duration in seconds as a float.

    Args:
        text: The string to check.

    Returns:
        The duration in seconds as a float if the pattern is found, otherwise None.
    """

    import re

    pattern = r"--SLEEP_ROW\((.+?)\)"  # Regex to match --SLEEP(duration)
    match = re.search(pattern, text)

    if match:
        try:
            duration = float(match.group(1))  # Extract the group and convert to float
            return duration
        except ValueError:
            # Handle cases where the extracted value is not a valid number
            return None
    else:
        return None


@attr.s
class YQLAdapterBase(BaseClassicAdapter[_DBA_YQL_BASE_DTO_TV]):
    def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        # Not useful.
        return None

    def _is_table_exists(self, table_ident: TableIdent) -> bool:
        # TODO?: use get_columns for this.
        return True

    _type_code_to_sa = {
        None: sa.TEXT,  # fallback
        "Int8": ydb_sa.types.Int8,
        "Int16": ydb_sa.types.Int16,
        "Int32": ydb_sa.types.Int32,
        "Int64": ydb_sa.types.Int64,
        "Uint8": ydb_sa.types.UInt8,
        "Uint16": ydb_sa.types.UInt16,
        "Uint32": ydb_sa.types.UInt32,
        "Uint64": ydb_sa.types.UInt64,
        "Float": sa.FLOAT,
        "Double": sa.FLOAT,
        "String": sa.TEXT,
        "Utf8": sa.TEXT,
        "Json": sa.TEXT,
        "Yson": sa.TEXT,
        "Uuid": sa.TEXT,
        "Date": sa.DATE,
        "Timestamp": dl_sqlalchemy_ydb.dialect.YqlTimestamp,
        "Datetime": dl_sqlalchemy_ydb.dialect.YqlDateTime,
        "Interval": dl_sqlalchemy_ydb.dialect.YqlInterval,
        "Bool": sa.BOOLEAN,
    }
    _type_code_to_sa = {
        **_type_code_to_sa,
        # Nullable types:
        **{name + "?": sa_type for name, sa_type in _type_code_to_sa.items() if name},
    }
    _type_code_to_sa_prefixes = {
        "Decimal(": sa.FLOAT,
    }

    def _cursor_column_to_sa(self, cursor_col: tuple[Any, ...], require: bool = True) -> Optional[SATypeSpec]:
        result = super()._cursor_column_to_sa(cursor_col)
        if result is not None:
            return result
        # Fallback: prefix
        type_code = cursor_col[1]
        for type_prefix, sa_type in self._type_code_to_sa_prefixes.items():
            if type_code.startswith(type_prefix):
                return sa_type
        if require:
            raise ValueError(f"Unknown type_code: {type_code!r}")
        return None

    _subselect_cursor_info_where_false: ClassVar[bool] = False

    def _get_row_converters(self, cursor_info: ExecutionStepCursorInfo) -> tuple[Optional[Callable[[Any], Any]], ...]:
        type_names_norm = [col[1].lower().replace("?", "") for col in cursor_info.raw_cursor_description]
        return tuple(
            dl_connector_ydb.core.base.row_converters.ROW_CONVERTERS.get(type_name_norm, None)
            for type_name_norm in type_names_norm
        )

    @classmethod
    def make_exc(  # TODO:  Move to ErrorTransformer
        cls,
        wrapper_exc: Exception,
        orig_exc: Exception | None,
        debug_query: str | None,
        inspector_query: str | None,
    ) -> tuple[type[exc.DatabaseQueryError], DBExcKWArgs]:
        exc_cls, kw = super().make_exc(wrapper_exc, orig_exc, debug_query, inspector_query)

        try:
            message = wrapper_exc.message  # type: ignore  # 2024-01-24 # TODO: "Exception" has no attribute "message"  [attr-defined]
        except Exception:
            pass
        else:
            kw["db_message"] = kw.get("db_message") or message

        return exc_cls, kw

    def get_engine_kwargs(self) -> dict:
        return {}

    @generic_profiler("db-full")
    def execute_by_steps(self, db_adapter_query: DBAdapterQuery) -> Generator[ExecutionStep, None, None]:
        """
        Generator that yielding messages with data chunks and execution meta-info.

        This override adds time.sleep(1) between each row for YDB connections to help debug timeouts.
        """
        query = db_adapter_query.query

        engine = self.get_db_engine(
            db_name=db_adapter_query.db_name,
            disable_streaming=db_adapter_query.disable_streaming,
        )

        # TODO FIX: Delegate query compilation for debug to error handler or make method of debug compilation
        from dl_core.connection_executors.adapters.sa_utils import (
            compile_query_for_debug,
            compile_query_for_inspector,
        )

        debug_query = compile_query_for_debug(query, engine.dialect)
        inspector_query = compile_query_for_inspector(query, engine.dialect)

        with (
            db_session_context(backend_type=self.get_backend_type(), db_engine=engine) as db_session,
            self.handle_execution_error(debug_query=debug_query, inspector_query=inspector_query),
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

            row_duration = extract_sleep_duration(inspector_query)
            LOGGER.info("YDB Debug row sleep duration: %s", row_duration)

            row_converters = self._get_row_converters(cursor_info=cursor_info)
            while True:
                LOGGER.info("Fetching 1 row (conn %s)", self.conn_id)
                with GenericProfiler("db-fetch"):
                    row = result.fetchone()

                if not row:
                    LOGGER.info("No rows remaining")
                    break

                processed_row = tuple(
                    (col_converter(val) if col_converter is not None and val is not None else val)
                    for val, col_converter in zip(row, row_converters, strict=True)
                )

                # Add 1 second sleep between each row for YDB timeout debugging
                if row_duration:
                    time.sleep(row_duration)

                LOGGER.debug("YDB debug: processed row")

                yield ExecutionStepDataChunk(
                    tuple(
                        processed_row,
                    )
                )

from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    TypeVar,
)

import attr
import sqlalchemy as sa
import ydb
import ydb_sqlalchemy.sqlalchemy as ydb_sa

from dl_core import exc
from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_models import TableIdent
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

DEFAULT_YDB_REQUEST_TIMEOUT_SEC = 80


@attr.s
class YQLAdapterBase(BaseClassicAdapter[_DBA_YQL_BASE_DTO_TV]):
    # TODO: bind timeout value to the COMMON_TIMEOUT_SEC app setting or create a new one
    execution_options: ClassVar[dict] = {
        "ydb_retry_settings": ydb.RetrySettings(retry_cancelled=True),
        "ydb_request_settings": (ydb.BaseRequestSettings().with_operation_timeout(DEFAULT_YDB_REQUEST_TIMEOUT_SEC)),
    }

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

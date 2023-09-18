from __future__ import annotations

import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import attr
import sqlalchemy as sa

from dl_core import exc
from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_models import TableIdent

if TYPE_CHECKING:
    from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO  # noqa: F401
    from dl_core.connection_executors.models.db_adapter_data import ExecutionStepCursorInfo
    from dl_core.connection_models import DBIdent
    from dl_core.connectors.base.error_transformer import DBExcKWArgs
    from dl_core.db.native_type import SATypeSpec


LOGGER = logging.getLogger(__name__)

_DBA_YQL_BASE_DTO_TV = TypeVar("_DBA_YQL_BASE_DTO_TV", bound="BaseSQLConnTargetDTO")


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
        "Int8": sa.INTEGER,
        "Int16": sa.INTEGER,
        "Int32": sa.INTEGER,
        "Int64": sa.INTEGER,
        "Uint8": sa.INTEGER,
        "Uint16": sa.INTEGER,
        "Uint32": sa.INTEGER,
        "Uint64": sa.INTEGER,
        "Float": sa.FLOAT,
        "Double": sa.FLOAT,
        "String": sa.TEXT,
        "Utf8": sa.TEXT,
        "Json": sa.TEXT,
        "Yson": sa.TEXT,
        "Uuid": sa.TEXT,
        "Date": sa.DATE,
        "Datetime": sa.DATETIME,
        "Timestamp": sa.DATETIME,
        "Interval": sa.INTEGER,
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

    def _cursor_column_to_sa(self, cursor_col: Tuple[Any, ...], require: bool = True) -> Optional[SATypeSpec]:
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

    @staticmethod
    def _convert_bytes(value: bytes) -> str:
        return value.decode("utf-8", errors="replace")

    @staticmethod
    def _convert_ts(value: int) -> datetime.datetime:
        return datetime.datetime.utcfromtimestamp(value / 1e6).replace(tzinfo=datetime.timezone.utc)

    def _get_row_converters(self, cursor_info: ExecutionStepCursorInfo) -> Tuple[Optional[Callable[[Any], Any]], ...]:
        type_names_norm = [col[1].lower().strip("?") for col in cursor_info.raw_cursor_description]
        return tuple(
            self._convert_bytes
            if type_name_norm == "string"
            else self._convert_ts
            if type_name_norm == "timestamp"
            else None
            for type_name_norm in type_names_norm
        )

    @classmethod
    def make_exc(  # TODO:  Move to ErrorTransformer
        cls, wrapper_exc: Exception, orig_exc: Optional[Exception], debug_compiled_query: Optional[str]
    ) -> Tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
        exc_cls, kw = super().make_exc(wrapper_exc, orig_exc, debug_compiled_query)

        try:
            message = getattr(wrapper_exc, "message")
        except Exception:
            pass
        else:
            kw["db_message"] = kw.get("db_message") or message

        return exc_cls, kw

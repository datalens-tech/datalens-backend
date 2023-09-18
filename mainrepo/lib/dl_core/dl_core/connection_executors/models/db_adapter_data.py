from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import attr
from sqlalchemy import sql as sasql

if TYPE_CHECKING:
    from dl_constants.enums import IndexKind
    from dl_constants.types import TJSONExt
    from dl_core.db.native_type import GenericNativeType


class ExecutionStep:
    pass


@attr.s(frozen=True)
class ExecutionStepCursorInfo(ExecutionStep):  # type: ignore  # TODO: fix
    cursor_info: Dict = attr.ib(factory=lambda: {})
    # Definitely not to be serialized:
    raw_cursor_description = attr.ib(default=None)  # type: ignore  # TODO: fix
    raw_engine = attr.ib(default=None)  # type: ignore  # TODO: fix


@attr.s(frozen=True)
class ExecutionStepDataChunk(ExecutionStep):
    chunk: Sequence[Sequence] = attr.ib()


_DB_ADAPTER_QUERY_TV = TypeVar("_DB_ADAPTER_QUERY_TV", bound="DBAdapterQuery")


@attr.s(frozen=True)
class DBAdapterQuery:
    query: Union[sasql.Select, str] = attr.ib()
    db_name: Optional[str] = attr.ib(default=None)
    debug_compiled_query: Optional[str] = attr.ib(default=None)
    chunk_size: Optional[int] = attr.ib(default=None)
    connector_specific_params: Optional[Dict[str, TJSONExt]] = attr.ib(default=None)
    # Use-case: `explain ...` queries are incompatible with streaming (and
    # server-side cursors in general) because psycopg2 prepends 'DECLARE ...
    # CURSOR WITHOUT HOLD FOR ...' to the statement.
    # NOTE: if there are more flags, replace the whole `disable_streaming` with a FrozenDict.
    disable_streaming: bool = attr.ib(default=False)
    trusted_query: bool = attr.ib(default=False)
    is_ddl_dml_query: bool = attr.ib(default=False)
    is_dashsql_query: bool = attr.ib(default=False)

    def get_effective_chunk_size(self, default_chunk_size: int) -> int:
        if self.chunk_size is None:
            return default_chunk_size
        else:
            return self.chunk_size

    def clone(self: _DB_ADAPTER_QUERY_TV, **kwargs: Any) -> _DB_ADAPTER_QUERY_TV:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True, slots=True, auto_attribs=True)
class RawColumnInfo:
    name: str
    title: Optional[str]
    nullable: bool
    native_type: "GenericNativeType"


@attr.s(frozen=True, slots=True, auto_attribs=True)
class RawIndexInfo:
    columns: Tuple[str, ...]
    unique: bool
    kind: Optional[IndexKind]


@attr.s(frozen=True, slots=True, auto_attribs=True)
class RawSchemaInfo:
    columns: Tuple[RawColumnInfo, ...]
    indexes: Optional[Tuple[RawIndexInfo, ...]] = None

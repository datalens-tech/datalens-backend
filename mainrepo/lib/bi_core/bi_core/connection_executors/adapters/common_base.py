from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
    Type,
    TypeVar,
)

import sqlalchemy as sa
from sqlalchemy.engine.default import DefaultDialect

from bi_constants.enums import (
    ConnectionType,
    SourceBackendType,
)
from bi_core.backend_types import get_backend_type
from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI

if TYPE_CHECKING:
    from bi_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO  # noqa: F401


_DIALECT_STRINGS: dict[ConnectionType, str] = {}  # Filled from connectors


def register_dialect_string(conn_type: ConnectionType, dialect_str: str) -> None:
    if (registered_dialect_string := _DIALECT_STRINGS.get(conn_type)) is not None:
        assert registered_dialect_string == dialect_str
    else:
        _DIALECT_STRINGS[conn_type] = dialect_str


def get_dialect_string(conn_type: ConnectionType) -> str:
    return _DIALECT_STRINGS[conn_type]


def get_dialect_for_conn_type(conn_type: ConnectionType) -> DefaultDialect:
    engine = sa.create_engine(f"{get_dialect_string(conn_type)}://", strategy="mock", executor=lambda *_, **__: None)
    engine = engine.execution_options(compiled_cache=None)
    return engine.dialect


_DBA_TV = TypeVar("_DBA_TV", bound="CommonBaseDirectAdapter")
_TARGET_DTO_TV = TypeVar("_TARGET_DTO_TV", bound="ConnTargetDTO")


class CommonBaseDirectAdapter(Generic[_TARGET_DTO_TV], metaclass=abc.ABCMeta):
    # TODO FIX: May be use dialect strings right here???
    conn_type: ClassVar[ConnectionType]  # Kostyl to use `get_dialect_string`  # FIXME: Switch to backend_type here

    def get_backend_type(self) -> SourceBackendType:
        return get_backend_type(self.conn_type)

    @classmethod
    def get_dialect_str(cls) -> str:
        return get_dialect_string(cls.conn_type)

    @classmethod
    @abc.abstractmethod
    def create(
        cls: Type[_DBA_TV], target_dto: _TARGET_DTO_TV, req_ctx_info: DBAdapterScopedRCI, default_chunk_size: int
    ) -> _DBA_TV:
        """Generic way to create"""

    @classmethod
    def get_dialect(cls) -> DefaultDialect:
        return get_dialect_for_conn_type(cls.conn_type)

    def compile_query_for_execution(
        self, query: sa.sql.Select | str, dialect: Optional[sa.engine.Dialect] = None
    ) -> str:
        """Should not be used unless it is impossible to avoid"""
        if isinstance(query, str):
            return query

        if dialect is None:
            dialect = self.get_dialect()

        compiled_query = query.compile(
            dialect=dialect,
            compile_kwargs={"literal_binds": True},
        )

        # TODO: check the `dialect dbapi paramstyle`
        query_text = str(compiled_query)

        return query_text

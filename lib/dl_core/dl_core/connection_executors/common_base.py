from __future__ import annotations

import abc
import contextlib
import enum
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    FrozenSet,
    Generator,
    List,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
)

import attr
from sqlalchemy.sql.elements import ClauseElement

from dl_api_commons.base_models import RequestContextInfo
from dl_constants.enums import UserDataType
from dl_constants.exc import DLBaseException
from dl_constants.types import TBIDataValue
from dl_core.connection_executors.models.common import RemoteQueryExecutorData
from dl_core.connection_executors.models.db_adapter_data import RawSchemaInfo
from dl_core.connection_models import (
    ConnDTO,
    ConnectOptions,
)
from dl_core.db import (
    SAMPLE_ID_COLUMN_NAME,
    IndexInfo,
    SchemaColumn,
    SchemaInfo,
    TypeTransformer,
    get_type_transformer,
)
from dl_core.exc import UnsupportedNativeTypeError


if TYPE_CHECKING:
    from dl_constants.types import TJSONExt
    from dl_core.connections_security.base import ConnectionSecurityManager  # noqa
    from dl_core.services_registry.top_level import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class ExecutionMode(enum.Enum):
    """Will be turned into something like (is_safe: bool, is_async: bool, via_qe: bool)"""

    DIRECT = enum.auto()
    RQE = enum.auto()


@attr.s
class ConnExecutorQuery:
    query: ClauseElement | str = attr.ib()
    user_types: Optional[List[UserDataType]] = attr.ib(default=None)
    debug_compiled_query: Optional[str] = attr.ib(default=None)
    chunk_size: Optional[int] = attr.ib(default=None)
    connector_specific_params: Optional[Mapping[str, TJSONExt]] = attr.ib(default=None)
    # TODO FIX: We really need it in query?
    db_name: Optional[str] = attr.ib(default=None)
    autodetect_user_types: bool = attr.ib(default=False)
    trusted_query: bool = attr.ib(default=False)
    is_ddl_dml_query: bool = attr.ib(default=False)
    is_dashsql_query: bool = attr.ib(default=False)


_CONN_EXEC_TV = TypeVar("_CONN_EXEC_TV", bound="ConnExecutorBase")


@attr.s(cmp=False, hash=False)
class ConnExecutorBase(metaclass=abc.ABCMeta):
    default_chunk_size: ClassVar[int] = 100
    supported_exec_mode: ClassVar[FrozenSet[ExecutionMode]] = frozenset(ExecutionMode)

    _conn_dto: ConnDTO = attr.ib()
    _conn_options: ConnectOptions = attr.ib()
    _conn_hosts_pool: Sequence[str] = attr.ib()
    _host_fail_callback: Callable = attr.ib()
    _req_ctx_info: Optional[RequestContextInfo] = attr.ib()
    _exec_mode: ExecutionMode = attr.ib()
    _sec_mgr: "ConnectionSecurityManager" = attr.ib()
    _remote_qe_data: Optional[RemoteQueryExecutorData] = attr.ib()
    _services_registry: Optional[ServicesRegistry] = attr.ib(
        kw_only=True, default=None
    )  # Do not use. To be deprecated. Somehow.
    _is_initialized: bool = attr.ib(init=False, default=False)

    @_exec_mode.validator
    def _validate_exc_mode(self, _: Any, value: Any) -> None:
        if value not in self.supported_exec_mode:
            raise ValueError(f"Unsupported execution mode {value} for {type(self).__qualname__}")

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    @property
    def type_transformer(self) -> TypeTransformer:
        return get_type_transformer(self._conn_dto.conn_type)

    def is_conn_dto_equals(self, another: ConnDTO) -> bool:
        try:
            return self._conn_dto == another
        except Exception:  # noqa
            LOGGER.exception("Exception during connection DTO comparision")
            return False

    def is_context_info_equals(self, another: RequestContextInfo) -> bool:
        try:
            return self._req_ctx_info == another
        except Exception:  # noqa
            LOGGER.exception("Exception during request context info comparision")
            return False

    def cast_row_to_output(self, row: Sequence, user_types: Optional[Sequence[UserDataType]]) -> Sequence[TBIDataValue]:
        if user_types is None:
            return row

        if len(user_types) != len(row):
            raise ValueError(f"Length of user_types {len(user_types)} != length of row {len(row)}")
        return tuple(
            self.type_transformer.cast_for_output(val, user_t=user_type) for val, user_type in zip(row, user_types)
        )

    def create_schema_info_from_raw_schema_info(
        self,
        raw_schema_info: RawSchemaInfo,
        require_all: bool = False,
    ) -> SchemaInfo:
        schema = []
        type_transformer = self.type_transformer

        for raw_column_info in raw_schema_info.columns:
            if raw_column_info.name == SAMPLE_ID_COLUMN_NAME:
                continue

            try:
                user_type = type_transformer.type_native_to_user(native_t=raw_column_info.native_type)
            except UnsupportedNativeTypeError:
                if require_all:
                    raise
                LOGGER.warning("Unable to detect type of field: %s", raw_column_info)
                user_type = UserDataType.unsupported

            schema_col = SchemaColumn(
                name=raw_column_info.name,
                title=raw_column_info.title or raw_column_info.name,
                user_type=user_type,
                nullable=raw_column_info.nullable,
                native_type=raw_column_info.native_type,
            )
            schema.append(schema_col)

        index_info_set: Optional[FrozenSet[IndexInfo]] = None

        if raw_schema_info.indexes is not None:
            index_info_set = frozenset(
                IndexInfo(
                    columns=raw_idx_info.columns,
                    kind=raw_idx_info.kind,
                )
                for raw_idx_info in raw_schema_info.indexes
            )

        return SchemaInfo(
            schema=schema,
            indexes=index_info_set,
        )

    @contextlib.contextmanager
    def _postprocess_db_excs(self) -> Generator[None, None, None]:
        try:
            yield
        except DLBaseException as exc:
            if self._conn_options.pass_db_messages_to_user:
                for key in ("query", "db_message"):
                    value = exc.debug_info.get(key)
                    if exc.details.get(key) is None and value is not None:
                        exc.details[key] = value
            raise

    def mutate_for_dashsql(self, db_params: Optional[Dict[str, str]] = None):  # type: ignore  # TODO: fix
        """
        A place to do CE-specific alterations for DashSQL support.
        """
        return self

    def clone(self: _CONN_EXEC_TV, **kwargs: Any) -> _CONN_EXEC_TV:
        return attr.evolve(self, **kwargs)

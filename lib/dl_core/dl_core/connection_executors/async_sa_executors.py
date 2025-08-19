from __future__ import annotations

import abc
import asyncio
import functools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generic,
    Optional,
    Sequence,
    TypeVar,
)

import attr
from typing_extensions import final

from dl_api_commons.base_models import RequestContextInfo
from dl_constants.enums import UserDataType
from dl_core import exc
from dl_core.connection_executors.adapters.adapters_base import SyncDirectDBAdapter
from dl_core.connection_executors.adapters.async_adapters_base import (
    AsyncDBAdapter,
    AsyncDirectDBAdapter,
    AsyncRawExecutionResult,
    AsyncRawJsonExecutionResult,
)
from dl_core.connection_executors.adapters.async_adapters_remote import RemoteAsyncAdapter
from dl_core.connection_executors.adapters.async_adapters_sync_wrapper import AsyncWrapperForSyncAdapter
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.async_base import (
    AsyncConnExecutorBase,
    AsyncExecutionResult,
)
from dl_core.connection_executors.common_base import (
    ConnExecutorQuery,
    ExecutionMode,
)
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connection_models.common_models import TableDefinition
from dl_core.db import SchemaInfo
from dl_type_transformer.exc import UnsupportedNativeTypeError
from dl_utils.aio import ContextVarExecutor


if TYPE_CHECKING:
    from dl_constants.types import TBIChunksGen
    from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
    from dl_core.connection_models.common_models import (
        DBIdent,
        SchemaIdent,
        TableIdent,
    )
    from dl_dashsql.typed_query.primitives import (
        TypedQuery,
        TypedQueryRaw,
        TypedQueryRawResult,
        TypedQueryResult,
    )


LOGGER = logging.getLogger(__name__)

_DBA_TV = TypeVar("_DBA_TV", bound=CommonBaseDirectAdapter)


def _dba_pool_revolver_wrapper(func: Callable) -> Callable:
    @functools.wraps(func)
    async def method_wrapper(self: DefaultSqlAlchemyConnExecutor, *args, **kwargs):  # type: ignore  # TODO: fix
        # Not sure do we have to reset index on every call
        # self._dba_attempt_index = 0

        while self._dba_attempt_index < len(self._async_dba_pool):  # type: ignore  # TODO: fix
            try:
                return await func(self, *args, **kwargs)
            except (exc.SourceConnectError, exc.DatabaseOperationalError) as ex:
                LOGGER.info("Retriable exception caught: %s", type(ex), exc_info=True)

                if self._conn_hosts_pool:
                    self._host_fail_callback(self._conn_hosts_pool[self._dba_attempt_index])

                self._dba_attempt_index += 1
                if self._dba_attempt_index >= len(self._async_dba_pool):  # type: ignore  # TODO: fix
                    raise
            except Exception as ex:
                LOGGER.info("Non-retriable dba exception %s: %s", type(ex), ex, exc_info=True)
                raise

    return method_wrapper


def _postprocess_db_excs_wrapper(func: Callable) -> Callable:
    @functools.wraps(func)
    async def method_wrapper(self: DefaultSqlAlchemyConnExecutor, *args, **kwargs):  # type: ignore  # TODO: fix
        with self._postprocess_db_excs():
            return await func(self, *args, **kwargs)

    return method_wrapper


def _common_exec_wrapper(func: Callable) -> Callable:
    func = _dba_pool_revolver_wrapper(func)
    func = _postprocess_db_excs_wrapper(func)
    return func


# TODO FIX: Encapsulate STRAIGHTFORWARD/WRAPPED state to single object to simplify code
@attr.s(cmp=False, hash=False)
class DefaultSqlAlchemyConnExecutor(AsyncConnExecutorBase, Generic[_DBA_TV], metaclass=abc.ABCMeta):
    """
    Base class that use SA adapter. To execute queries.
    Executor has 2 use cases in DIRECT execution mode (in RQE execution mode this is not true):
      STRAIGHTFORWARD: Direct execution in async environment
      WRAPPED: Wrapping by sync wrapper in sync environment
    To use STRAIGHTFORWARD - pass TPE in constructor. It will be used to execute sync operations.
    To use WRAPPED - do not pass TPE. Assumed that executor will be used by sync wrapper.
      All direct operations will cause an exception in this case.
      In this mode async database adapter will not be created.
      Adapter close process is delegated to

    Why do we need it: to keep initialization creation of DBA in one place for sync and async environments.
    """

    TARGET_ADAPTER_CLS: ClassVar[type[_DBA_TV]]  # type: ignore  # 2024-01-24 # TODO: ClassVar cannot contain type variables  [misc]
    REMOTE_ADAPTER_CLS: ClassVar[Optional[type[RemoteAsyncAdapter]]] = None

    # Constructor attributes
    _tpe: Optional[ContextVarExecutor] = attr.ib()

    # Internals
    _async_dba_pool: Optional[list[AsyncDBAdapter]] = attr.ib(init=False, default=None)
    _sync_dba: Optional[SyncDirectDBAdapter] = attr.ib(init=False, default=None)
    _initialization_lock = attr.ib(init=False, factory=asyncio.Lock)
    _dba_attempt_index: int = attr.ib(init=False, default=0)

    @classmethod
    def is_pure_async(cls) -> bool:
        return issubclass(cls.TARGET_ADAPTER_CLS, AsyncDirectDBAdapter)

    @classmethod
    def use_custom_rqe(cls) -> bool:
        return False

    @property
    def _target_dba(self) -> AsyncDBAdapter:
        if self._async_dba_pool is None:
            raise ValueError("Executor created in WRAPPED mode (no TPE was provided). Async DBA was not created.")
        return self._async_dba_pool[self._dba_attempt_index]

    def _get_sync_sa_adapter(self) -> Optional[SyncDirectDBAdapter]:
        """Method for sync conn executor wrapper. Please do not use it in other places."""
        if not self._is_initialized:
            raise ValueError("Connection executor was not initiated")
        return self._sync_dba

    @abc.abstractmethod
    async def _make_target_conn_dto_pool(self) -> Sequence[ConnTargetDTO]:
        """
        This method will be used for creating target connection DTO to pass it to RQE.
        This method also should be used in `_make_sa_adapter()` to create adapter for DIRECT mode.
        :return: Target connection DTO
        """

    @final
    async def initialize(self) -> None:
        async with self._initialization_lock:
            if self._is_initialized:
                return
            else:
                await self._initialize()
                self._is_initialized = True

    @final
    async def _initialize(self) -> None:
        target_conn_dto_pool = await self._make_target_conn_dto_pool()
        assert target_conn_dto_pool

        req_ctx_info = self._req_ctx_info or RequestContextInfo.create_empty()

        if self._exec_mode == ExecutionMode.DIRECT:
            if issubclass(self.TARGET_ADAPTER_CLS, SyncDirectDBAdapter):
                sync_dba = self.TARGET_ADAPTER_CLS.create(
                    target_dto=target_conn_dto_pool[0],
                    req_ctx_info=DBAdapterScopedRCI.from_full_rci(req_ctx_info),
                    default_chunk_size=self.default_chunk_size,
                )
                self._sync_dba = sync_dba

                # Normal use case
                if self._tpe is not None:
                    self._async_dba_pool = [
                        AsyncWrapperForSyncAdapter(
                            sync_adapter=sync_dba,
                            tpe=self._tpe,
                        )
                    ]
                # Sync wrapper use-case
                else:
                    LOGGER.info("TPE was not provided async wrapper for DB adapter will not be created")
            elif issubclass(self.TARGET_ADAPTER_CLS, AsyncDirectDBAdapter):
                self._async_dba_pool = [
                    self.TARGET_ADAPTER_CLS.create(
                        target_dto=target_conn_dto,
                        req_ctx_info=DBAdapterScopedRCI.from_full_rci(req_ctx_info),
                        default_chunk_size=self.default_chunk_size,
                    )
                    for target_conn_dto in target_conn_dto_pool
                ]
            else:
                raise TypeError(f"Can not create DBA TARGET_ADAPTER_CLS={self.TARGET_ADAPTER_CLS}")

        elif self._exec_mode == ExecutionMode.RQE:
            if self._remote_qe_data is None:
                raise ValueError("Can not initialize: no remote QE data was provided")

            remote_adapter_cls = self.REMOTE_ADAPTER_CLS or RemoteAsyncAdapter
            self._async_dba_pool = [
                remote_adapter_cls(
                    target_dto=target_conn_dto,
                    rqe_data=self._remote_qe_data,
                    req_ctx_info=DBAdapterScopedRCI.from_full_rci(req_ctx_info),
                    dba_cls=self.TARGET_ADAPTER_CLS,
                    conn_options=self._conn_options,
                )
                for target_conn_dto in target_conn_dto_pool
            ]

        else:
            raise NotImplementedError(f"Unsupported execution mode {self._exec_mode}")

    def executor_query_to_db_adapter_query(self, conn_exec_query: ConnExecutorQuery) -> DBAdapterQuery:
        return DBAdapterQuery(
            query=conn_exec_query.query,
            chunk_size=conn_exec_query.chunk_size,
            db_name=conn_exec_query.db_name,
            debug_compiled_query=conn_exec_query.debug_compiled_query,
            connector_specific_params=conn_exec_query.connector_specific_params,
            trusted_query=conn_exec_query.trusted_query,
            is_ddl_dml_query=conn_exec_query.is_ddl_dml_query,
            is_dashsql_query=conn_exec_query.is_dashsql_query,
        )

    @_common_exec_wrapper
    async def _execute_query(self, query: DBAdapterQuery) -> AsyncRawExecutionResult | AsyncRawJsonExecutionResult:
        return await self._target_dba.execute(query)

    def _autodetect_user_types(self, raw_cursor_info: dict) -> Optional[list[UserDataType]]:
        db_types = raw_cursor_info.get("db_types")
        if not db_types:
            return None
        tt = self.type_transformer

        result = []
        for native_type in db_types:
            bi_type = UserDataType.unsupported

            if native_type:
                try:
                    bi_type = tt.type_native_to_user(native_type)
                except UnsupportedNativeTypeError:
                    pass

            result.append(bi_type)

        return result

    async def _execute_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        return await self._target_dba.execute_typed_query(typed_query=typed_query)

    async def _execute_typed_query_raw(self, typed_query_raw: TypedQueryRaw) -> TypedQueryRawResult:
        return await self._target_dba.execute_typed_query_raw(typed_query_raw=typed_query_raw)

    async def _execute(self, query: ConnExecutorQuery) -> AsyncExecutionResult:
        raw_result = await self._execute_query(self.executor_query_to_db_adapter_query(query))

        user_types = query.user_types
        if query.autodetect_user_types:
            user_types = self._autodetect_user_types(raw_result.raw_cursor_info)

        result_footer: dict[str, Any] = {}

        async def data_generator() -> TBIChunksGen:
            async for chunk in raw_result.raw_chunk_generator:
                yield [self.cast_row_to_output(row, query.user_types) for row in chunk]

        return AsyncExecutionResult(
            cursor_info=raw_result.raw_cursor_info,
            result=data_generator(),
            user_types=user_types,
            result_footer=result_footer,
        )

    @_common_exec_wrapper
    async def _test(self) -> None:
        await self._target_dba.test()

    @_common_exec_wrapper
    async def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return await self._target_dba.get_db_version(db_ident)

    @_common_exec_wrapper
    async def _get_schema_names(self, db_ident: DBIdent) -> list[str]:
        return await self._target_dba.get_schema_names(db_ident)

    @_common_exec_wrapper
    async def _get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        return await self._target_dba.get_tables(schema_ident)

    @_common_exec_wrapper
    async def _get_table_schema_info(self, table_def: TableDefinition) -> SchemaInfo:
        raw_table_info = await self._target_dba.get_table_info(table_def, self._conn_options.fetch_table_indexes)
        return self.create_schema_info_from_raw_schema_info(raw_table_info)

    @_common_exec_wrapper
    async def _is_table_exists(self, table_ident: TableIdent) -> bool:
        return await self._target_dba.is_table_exists(table_ident)

    async def _close(self) -> None:
        if self._async_dba_pool is not None:
            for dba in self._async_dba_pool:
                try:
                    # TODO FIX: May be make timeout
                    await dba.close()
                except Exception:
                    LOGGER.exception("Error during async adapter closing attempt")

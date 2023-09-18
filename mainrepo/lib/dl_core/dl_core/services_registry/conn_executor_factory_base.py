from __future__ import annotations

import abc
import asyncio
import functools
import itertools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

import attr
from typing_extensions import final

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connection_executors import (
    ExecutionMode,
    SyncWrapperForAsyncConnExecutor,
)
from dl_core.connection_executors.models.common import RemoteQueryExecutorData
from dl_core.connection_models import (
    ConnDTO,
    ConnectOptions,
)
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.us_connection_base import (
    ConnectionBase,
    ExecutorBasedMixin,
)
from dl_core.utils import FutureRef

if TYPE_CHECKING:
    from dl_core.connection_executors import (
        AsyncConnExecutorBase,
        SyncConnExecutorBase,
    )
    from dl_core.connections_security.base import ConnectionSecurityManager
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class CEFactoryError(Exception):
    pass


class ConnExecutorFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_async_conn_executor(self, conn: ConnectionBase) -> AsyncConnExecutorBase:
        pass

    @abc.abstractmethod
    def get_async_conn_executor_cls(self, conn: ExecutorBasedMixin) -> Type[AsyncConnExecutorBase]:
        pass

    @abc.abstractmethod
    def get_sync_conn_executor(self, conn: ConnectionBase) -> SyncConnExecutorBase:
        pass

    @abc.abstractmethod
    def close_sync(self) -> None:
        pass

    @abc.abstractmethod
    async def close_async(self) -> None:
        pass

    @property
    @abc.abstractmethod
    def req_ctx_info(self) -> RequestContextInfo:
        pass

    # TODO FIX: Move to ServiceRegistry
    @property
    @abc.abstractmethod
    def conn_security_manager(self) -> ConnectionSecurityManager:
        pass


_RESULT_TV = TypeVar("_RESULT_TV")


def ensure_env(async_env: bool = True) -> Callable[[Callable[..., _RESULT_TV]], Callable[..., _RESULT_TV]]:
    def real_deco(wrapped: Callable[..., _RESULT_TV]) -> Callable[..., _RESULT_TV]:
        @functools.wraps(wrapped)
        def wrapper(self: "BaseClosableExecutorFactory", *args: Any, **kwargs: Any) -> _RESULT_TV:
            if self._async_env != async_env:
                meth_env = "async" if async_env else "sync"
                factory_env = "async" if self._async_env else "sync"
                raise NotImplementedError(
                    f"Factory created for {factory_env} env "
                    f"but .{wrapped.__name__}() may be used only in {meth_env}"
                )
            return wrapped(self, *args, **kwargs)

        return wrapper

    return real_deco


@attr.s(frozen=True, auto_attribs=True)
class ConnExecutorRecipe:
    """Class that represent all data to create"""

    ce_cls: Type
    conn_dto: ConnDTO
    connect_options: ConnectOptions
    exec_mode: ExecutionMode
    rqe_data: Optional[RemoteQueryExecutorData]
    conn_hosts_pool: Sequence[str] = attr.ib(kw_only=True, converter=tuple)


@attr.s(frozen=True)
class BaseClosableExecutorFactory(ConnExecutorFactory, metaclass=abc.ABCMeta):
    """
    Base class for factories. Register all created conn executors.
    Mode depends on `async_env` flag. If it's on - all sync-related methods will throws NotImplementedError
      and vise-versa.
    If async_env=False - creates async CEs are not registered. Closing is delegate to sync wrappers.
    """

    # TODO CONSIDER: May be add some creation metadata here
    @attr.s(frozen=True, auto_attribs=True)
    class _CECreationResult:
        async_ce: AsyncConnExecutorBase
        sync_wrapper: Optional[SyncWrapperForAsyncConnExecutor]

    _map_recipe_created_ce_pair: Dict[ConnExecutorRecipe, List[_CECreationResult]] = attr.ib(init=False, factory=dict)

    _async_env: bool = attr.ib()
    _services_registry_ref: FutureRef[ServicesRegistry] = attr.ib()
    _entity_usage_checker: Optional[EntityUsageChecker] = attr.ib(default=None, kw_only=True)

    @property
    def req_ctx_info(self) -> RequestContextInfo:
        return self._services_registry_ref.ref.rci

    @property
    def async_env(self) -> bool:
        return self._async_env

    @final
    @ensure_env(async_env=True)
    def get_async_conn_executor(self, conn: ExecutorBasedMixin) -> AsyncConnExecutorBase:
        return self._get_or_create_conn_executor(conn, with_sync_wrapper=False).async_ce

    @final
    @ensure_env(async_env=False)
    def get_sync_conn_executor(self, conn: ExecutorBasedMixin) -> SyncConnExecutorBase:
        pair = self._get_or_create_conn_executor(conn, with_sync_wrapper=True)
        assert pair.sync_wrapper is not None
        return pair.sync_wrapper

    # TODO CONSIDER: May be add option to ignore cache?
    @final
    def _get_or_create_conn_executor(self, conn: ExecutorBasedMixin, with_sync_wrapper: bool) -> _CECreationResult:
        if self._entity_usage_checker is not None:
            assert isinstance(conn, ConnectionBase)
            self._entity_usage_checker.ensure_data_connection_can_be_used(rci=self.req_ctx_info, conn=conn)

        assert isinstance(conn, ExecutorBasedMixin)
        recipe = self._get_async_conn_executor_recipe(conn)
        created_list = self._map_recipe_created_ce_pair.setdefault(recipe, [])

        if len(created_list) > 0:
            LOGGER.info("CE by recipe was already created, returning cached one")
            return created_list[0]

        async_ce = self._cook_conn_executor(recipe, with_tpe=not with_sync_wrapper)
        sync_wrapper = (
            SyncWrapperForAsyncConnExecutor(
                # TODO CONSIDER: May be move to arguments
                loop=asyncio.get_event_loop(),
                async_conn_executor=async_ce,
            )
            if with_sync_wrapper
            else None
        )
        pair = self._CECreationResult(async_ce, sync_wrapper)
        created_list.append(pair)
        return pair

    @final
    @ensure_env(async_env=False)
    def close_sync(self) -> None:
        all_created_sync_wrappers = [
            pair.sync_wrapper
            for pair in itertools.chain(*self._map_recipe_created_ce_pair.values())
            if pair.sync_wrapper is not None
        ]
        for sync_wrapper in all_created_sync_wrappers:
            # noinspection PyBroadException
            try:
                sync_wrapper.close()
            except Exception:
                LOGGER.exception("Error during sync CE closing")

    @final
    @ensure_env(async_env=True)
    async def close_async(self) -> None:
        async def close_executor(executor: AsyncConnExecutorBase) -> None:
            # noinspection PyBroadException
            try:
                await executor.close()
            except Exception:
                LOGGER.exception("Error during async CE closing")

        all_created_ce = [pair.async_ce for pair in itertools.chain(*self._map_recipe_created_ce_pair.values())]
        await asyncio.gather(*[close_executor(ce) for ce in all_created_ce])

    @abc.abstractmethod
    def _get_async_conn_executor_recipe(
        self,
        conn: ExecutorBasedMixin,
    ) -> ConnExecutorRecipe:
        pass

    @abc.abstractmethod
    def _cook_conn_executor(self, recipe: ConnExecutorRecipe, with_tpe: bool) -> AsyncConnExecutorBase:
        pass

from __future__ import annotations

from collections import ChainMap
import logging
import random
from typing import (
    TYPE_CHECKING,
    ClassVar,
    FrozenSet,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

import attr

from bi_configs.rqe import (
    RQEBaseURL,
    RQEConfig,
)
from bi_core import connection_executors
from bi_core.connection_executors import ExecutionMode
from bi_core.connection_executors.async_base import AsyncConnExecutorBase
from bi_core.connection_executors.models.common import RemoteQueryExecutorData
from bi_core.connection_models import (
    ConnDTO,
    ConnectOptions,
    DefaultSQLDTO,
)
from bi_core.services_registry.conn_executor_factory_base import (
    BaseClosableExecutorFactory,
    CEFactoryError,
    ConnExecutorRecipe,
)
from bi_core.us_connection_base import ConnectionBase
from bi_utils.aio import ContextVarExecutor

if TYPE_CHECKING:
    from bi_core.connection_executors.common_base import ConnExecutorBase
    from bi_core.connections_security.base import ConnectionSecurityManager
    from bi_core.mdb_utils import MDBDomainManager
    from bi_core.services_registry.typing import (
        ConnectOptionsFactory,
        ConnectOptionsMutator,
    )
    from bi_core.us_connection_base import ExecutorBasedMixin


LOGGER = logging.getLogger(__name__)


# noinspection PyDataclass
@attr.s(frozen=True)
class DefaultConnExecutorFactory(BaseClosableExecutorFactory):
    rqe_config: Optional[RQEConfig] = attr.ib()
    tpe: Optional[ContextVarExecutor] = attr.ib()
    conn_sec_mgr: ConnectionSecurityManager = attr.ib()
    mdb_mgr: MDBDomainManager = attr.ib()

    is_bleeding_edge_user: bool = attr.ib(default=False)
    conn_cls_whitelist: Optional[FrozenSet[Type[ExecutorBasedMixin]]] = attr.ib(default=None)
    # User function that can override connect options.
    #  If factory returns `None` default connection connect options will be used
    connect_options_factory: Optional[ConnectOptionsFactory] = attr.ib(default=None)
    # User function that can alter connect options (whether they were generated
    # by factory or from the conn).
    connect_options_mutator: Optional[ConnectOptionsMutator] = attr.ib(default=None)
    force_non_rqe_mode: bool = attr.ib(default=False)

    DEFAULT_MAP_CONN_TYPE_SYNC_CE_TYPE: dict[Type[ConnectionBase], Type[ConnExecutorBase]] = {}

    DEFAULT_MAP_CONN_TYPE_ASYNC_CE_TYPE: dict[Type[ConnectionBase], Type[AsyncConnExecutorBase]] = {}

    SINGLE_HOST_RETRY_ATTEMPTS: ClassVar[int] = 3
    MAX_HOST_RETRY_ATTEMPTS: ClassVar[int] = 3

    # TODO FIX: Make typing/implementation more accurate
    def get_async_conn_executor_cls(self, conn: ExecutorBasedMixin) -> Type[AsyncConnExecutorBase]:
        map_conn_type_ce_type: MutableMapping[Type[ConnectionBase], Type[ConnExecutorBase]] = ChainMap()

        if self.async_env:
            map_conn_type_ce_type.update(self.DEFAULT_MAP_CONN_TYPE_SYNC_CE_TYPE)
            map_conn_type_ce_type.update(self.DEFAULT_MAP_CONN_TYPE_ASYNC_CE_TYPE)
        else:
            map_conn_type_ce_type.update(self.DEFAULT_MAP_CONN_TYPE_SYNC_CE_TYPE)

        if self.conn_cls_whitelist is not None:
            map_conn_type_ce_type = {
                conn_cls: ce_cls
                for conn_cls, ce_cls in map_conn_type_ce_type.items()
                if conn_cls in self.conn_cls_whitelist
            }

        try:
            ce = map_conn_type_ce_type[type(conn)]
            assert issubclass(ce, AsyncConnExecutorBase)
            return ce
        except KeyError:
            raise CEFactoryError(f"No executor for connection type {type(conn)}")

    def _get_conn_hosts_pool(self, dto: ConnDTO) -> Sequence[str]:
        if isinstance(dto, DefaultSQLDTO):
            hosts = list(dto.multihosts)

            random.shuffle(hosts)

            if len(hosts) == 1:
                hosts = hosts * self.SINGLE_HOST_RETRY_ATTEMPTS
            elif len(hosts) > self.MAX_HOST_RETRY_ATTEMPTS:
                LOGGER.info(f"Hosts list truncated from {len(hosts)} to {self.MAX_HOST_RETRY_ATTEMPTS}")
                hosts = hosts[: self.MAX_HOST_RETRY_ATTEMPTS]

            return tuple(hosts)
        else:
            return ()

    def _get_async_conn_executor_recipe(
        self,
        conn: ExecutorBasedMixin,
    ) -> ConnExecutorRecipe:
        # noinspection PyProtectedMember
        assert conn._context is self.req_ctx_info, "Divergence in RCI between CE factory and US connection"

        dto = conn.get_conn_dto()
        conn_hosts_pool = self._get_conn_hosts_pool(dto)

        executor_cls = self.get_async_conn_executor_cls(conn)
        exec_mode, rqe_data = self._get_exec_mode_and_rqe_attrs(conn, executor_cls)
        LOGGER.info("Connection executor exec_mode = %s", exec_mode)

        overridden_connect_options: Optional[ConnectOptions] = None
        if self.connect_options_factory is not None:
            overridden_connect_options = self.connect_options_factory(conn)

        connect_options: ConnectOptions
        if overridden_connect_options is not None:
            connect_options = overridden_connect_options
        else:
            connect_options = conn.get_conn_options()

        if self.connect_options_mutator is not None:
            connect_options = self.connect_options_mutator(connect_options)

        return ConnExecutorRecipe(
            ce_cls=executor_cls,
            conn_dto=dto,
            connect_options=connect_options,
            rqe_data=rqe_data,
            exec_mode=exec_mode,
            conn_hosts_pool=conn_hosts_pool,  # type: ignore  # TODO: fix
        )

    def _cook_conn_executor(self, recipe: ConnExecutorRecipe, with_tpe: bool) -> AsyncConnExecutorBase:
        def _conn_host_fail_callback_func(host: str):  # type: ignore  # TODO: fix
            LOGGER.info("DB host %s unavailable", host)

        executor_cls = recipe.ce_cls
        if issubclass(executor_cls, connection_executors.DefaultSqlAlchemyConnExecutor):
            # TODO FIX: log connection info
            LOGGER.info("Creating CE %s", executor_cls)
            return executor_cls(
                conn_dto=recipe.conn_dto,
                conn_options=recipe.connect_options,
                req_ctx_info=self.req_ctx_info,
                remote_qe_data=recipe.rqe_data,
                tpe=self.tpe if with_tpe else None,
                exec_mode=recipe.exec_mode,
                sec_mgr=self.conn_sec_mgr,
                mdb_mgr=self.mdb_mgr,
                conn_hosts_pool=recipe.conn_hosts_pool,
                host_fail_callback=_conn_host_fail_callback_func,
                services_registry=self._services_registry_ref.ref,  # Do not use. To be deprecated. Somehow.
            )
        else:
            raise CEFactoryError(f"Can not instantiate {executor_cls}")

    def _get_exec_mode_and_rqe_attrs(
        self, conn: ExecutorBasedMixin, executor_cls: Type[AsyncConnExecutorBase]
    ) -> Tuple[ExecutionMode, Optional[RemoteQueryExecutorData]]:
        conn_dto = conn.get_conn_dto()
        ce_cls = self.get_async_conn_executor_cls(conn)

        if not self.conn_sec_mgr.is_safe_connection(conn_dto):
            # Only RQE mode with external RQE supported for unsafe connection
            if ExecutionMode.RQE not in executor_cls.supported_exec_mode:
                raise CEFactoryError(
                    f"Connection classified as insecure,"
                    f" but configured executor class {executor_cls} does not support RQE exec mode"
                )
            return ExecutionMode.RQE, self._get_rqe_data(external=True)

        else:
            if self.force_non_rqe_mode:
                # WARNING: debug-only flag, might not even work
                return ExecutionMode.DIRECT, None
            elif ce_cls.is_pure_async():
                return ExecutionMode.DIRECT, None
            elif self.async_env:
                return ExecutionMode.RQE, self._get_rqe_data(external=False)
            else:
                return ExecutionMode.RQE, self._get_rqe_data(external=False)

    def _get_rqe_data(self, external: bool) -> RemoteQueryExecutorData:
        if self.rqe_config is None:
            raise CEFactoryError("RQE data was requested, but RQE config was not passed to CE factory")

        async_rqe_netloc: RQEBaseURL
        sync_rqe_netloc: RQEBaseURL

        LOGGER.info('RQE mode is "%s"', "external" if external else "internal")
        if external:
            async_rqe_netloc = self.rqe_config.ext_async_rqe
            sync_rqe_netloc = self.rqe_config.ext_sync_rqe
        else:
            async_rqe_netloc = self.rqe_config.int_async_rqe
            sync_rqe_netloc = self.rqe_config.int_sync_rqe

        return RemoteQueryExecutorData(
            hmac_key=self.rqe_config.hmac_key,
            async_protocol=async_rqe_netloc.scheme,
            async_host=async_rqe_netloc.host,
            async_port=async_rqe_netloc.port,
            sync_protocol=sync_rqe_netloc.scheme,
            sync_host=sync_rqe_netloc.host,
            sync_port=sync_rqe_netloc.port,
        )

    @property
    def conn_security_manager(self) -> ConnectionSecurityManager:
        return self.conn_sec_mgr

    def clone(self, **kwargs):  # type: ignore  # TODO: fix
        return attr.evolve(self, **kwargs)


def register_sync_conn_executor_class(conn_cls: Type[ConnectionBase], sync_ce_cls: Type[ConnExecutorBase]) -> None:
    DefaultConnExecutorFactory.DEFAULT_MAP_CONN_TYPE_SYNC_CE_TYPE[conn_cls] = sync_ce_cls


def register_async_conn_executor_class(
    conn_cls: Type[ConnectionBase], async_ce_cls: Type[AsyncConnExecutorBase]
) -> None:
    DefaultConnExecutorFactory.DEFAULT_MAP_CONN_TYPE_ASYNC_CE_TYPE[conn_cls] = async_ce_cls

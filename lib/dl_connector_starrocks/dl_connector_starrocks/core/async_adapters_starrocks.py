import logging
from typing import TypeVar

import attr

from dl_core.connection_executors.adapters.adapter_actions.async_base import AsyncDBVersionAdapterAction
from dl_core.connection_executors.adapters.adapter_actions.db_version import AsyncDBVersionAdapterActionViaFunctionQuery
from dl_core.connection_executors.adapters.async_adapters_base import AsyncDirectDBAdapter
from dl_core.connection_executors.adapters.mixins import (
    WithDatabaseNameOverride,
    WithMinimalCursorInfo,
    WithNoneRowConverters,
)
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI

# StarRocks is MySQL-compatible, so we can use MySQL query compilation
from dl_connector_starrocks.core.adapters_base_starrocks import BaseStarRocksAdapter
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


LOGGER = logging.getLogger(__name__)

_DBA_ASYNC_STARROCKS_TV = TypeVar("_DBA_ASYNC_STARROCKS_TV", bound="AsyncStarRocksAdapter")


@attr.s(cmp=False, kw_only=True)
class AsyncStarRocksAdapter(
    WithDatabaseNameOverride,
    WithNoneRowConverters,
    # ETBasedExceptionMaker,
    BaseStarRocksAdapter,
    AsyncDirectDBAdapter,
    WithMinimalCursorInfo,
):
    _req_ctx_info: DBAdapterScopedRCI = attr.ib()
    _default_chunk_size: int = attr.ib()

    # _engines: AsyncCache[aiomysql.sa.Engine] = attr.ib(default=attr.Factory(AsyncCache))

    # _error_transformer = async_starrocks_db_error_transformer

    # EXTRA_EXC_CLS = (
    #     OperationalError,
    #     ProgrammingError,
    #     RuntimeError,
    # )

    def _make_async_db_version_action(self) -> AsyncDBVersionAdapterAction:
        return AsyncDBVersionAdapterActionViaFunctionQuery(async_adapter=self)

    @classmethod
    def create(
        cls: type[_DBA_ASYNC_STARROCKS_TV],
        target_dto: StarRocksConnTargetDTO,
        req_ctx_info: DBAdapterScopedRCI,
        default_chunk_size: int,
    ) -> _DBA_ASYNC_STARROCKS_TV:
        return cls(target_dto=target_dto, req_ctx_info=req_ctx_info, default_chunk_size=default_chunk_size)

    def get_default_db_name(self) -> str | None:
        return self._target_dto.db_name

    def get_target_host(self) -> str | None:
        return self._target_dto.host

    def _get_ssl_ctx(self, force_ssl: bool = False) -> dict | None:
        # TODO: Add SSL support for StarRocks if needed
        # For MVP, we don't support SSL
        return None

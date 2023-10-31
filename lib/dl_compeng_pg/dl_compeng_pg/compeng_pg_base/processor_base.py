from __future__ import annotations

import abc
from typing import (
    Generic,
    Optional,
    TypeVar,
)

import attr

from dl_compeng_pg.compeng_pg_base.exec_adapter_base import PostgreSQLExecAdapterAsync
from dl_compeng_pg.compeng_pg_base.pool_base import BasePgPoolWrapper
from dl_constants.enums import UserDataType
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.data_processing.cache.utils import (
    CompengOptionsBuilder,
    DatasetOptionsBuilder,
)
from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.processing.db_base.processor_base import ExecutorBasedOperationProcessor


_POOL_TV = TypeVar("_POOL_TV", bound=BasePgPoolWrapper)
_CONN_TV = TypeVar("_CONN_TV")


@attr.s
class PostgreSQLOperationProcessor(ExecutorBasedOperationProcessor, Generic[_POOL_TV, _CONN_TV], metaclass=abc.ABCMeta):
    _pg_pool: _POOL_TV = attr.ib()
    _task_timeout: Optional[int] = attr.ib(default=None)
    _pg_conn: Optional[_CONN_TV] = attr.ib(init=False, default=None)
    _default_cache_ttl_config: CacheTTLConfig = attr.ib(factory=CacheTTLConfig)

    def _make_cache_options_builder(self) -> DatasetOptionsBuilder:
        assert self._default_cache_ttl_config is not None
        return CompengOptionsBuilder(default_ttl_config=self._default_cache_ttl_config)

    def _make_db_ex_adapter(self) -> Optional[ProcessorDbExecAdapterBase]:
        # The adapter has to be initialized asynchronously, so don't create it here yet
        return None

    @property
    def db_ex_adapter(self) -> PostgreSQLExecAdapterAsync:
        assert self._db_ex_adapter is not None
        # Add the sub-type condition because it needs to support DDL actions
        assert isinstance(self._db_ex_adapter, PostgreSQLExecAdapterAsync)
        return self._db_ex_adapter

    @abc.abstractmethod
    async def start(self) -> None:
        """Prepare for work."""

    @abc.abstractmethod
    async def end(self) -> None:
        """Cleanup."""

    async def ping(self) -> Optional[int]:
        ctx = OpExecutionContext(processing_id="", streams=[], operations=[])
        result = await self.db_ex_adapter.scalar(
            "select 1",
            user_type=UserDataType.integer,
            ctx=ctx,
        )
        assert result is None or isinstance(result, int)
        return result

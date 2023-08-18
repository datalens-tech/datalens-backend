from __future__ import annotations

from typing import Callable, ClassVar, TYPE_CHECKING

from bi_constants.enums import CreateDSFrom, ConnectionType
from bi_core.data_source.sql import DataSource
from bi_core.db import SchemaInfo

if TYPE_CHECKING:
    from bi_core.connection_executors.sync_base import SyncConnExecutorBase


class SolomonDataSource(DataSource):
    preview_enabled: ClassVar[bool] = False
    supports_offset: ClassVar[bool] = False

    conn_type = ConnectionType.solomon

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return False

    def get_schema_info(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> SchemaInfo:
        raise NotImplementedError()

    @property
    def default_title(self) -> str:
        return ''

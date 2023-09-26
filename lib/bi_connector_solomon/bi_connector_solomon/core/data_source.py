from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
)

from dl_constants.enums import DataSourceType
from dl_core.data_source.sql import DataSource
from dl_core.db import SchemaInfo

from bi_connector_solomon.core.constants import CONNECTION_TYPE_SOLOMON


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


class SolomonDataSource(DataSource):
    preview_enabled: ClassVar[bool] = False
    supports_offset: ClassVar[bool] = False

    conn_type = CONNECTION_TYPE_SOLOMON

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return False

    def get_schema_info(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> SchemaInfo:
        raise NotImplementedError()

    @property
    def default_title(self) -> str:
        return ""

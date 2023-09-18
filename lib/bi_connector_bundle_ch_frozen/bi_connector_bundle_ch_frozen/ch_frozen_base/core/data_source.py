from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
)

from dl_constants.enums import CreateDSFrom
from dl_core import exc
from dl_core.data_source import StandardSQLDataSource
from dl_core.data_source.sql import SubselectDataSource
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_ch_filtered.base.core.data_source import (
    ClickHouseFilteredDataSourceBase,
    ClickHouseTemplatedSubselectDataSource,
)
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.constants import (
    SOURCE_TYPE_CH_FROZEN_SOURCE,
    SOURCE_TYPE_CH_FROZEN_SUBSELECT,
)

if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


class ClickHouseFrozenDataSourceBase(ClickHouseTemplatedSubselectDataSource, ClickHouseFilteredDataSourceBase):
    preview_enabled: ClassVar[bool] = True

    def _check_existence(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> bool:
        return True

    def _is_allowed_subselect(self) -> bool:
        subselect_titles = (subselect_template.title for subselect_template in self.connection.subselect_templates)
        return self.table_name in subselect_titles

    def _is_allowed_table(self) -> bool:
        return self.table_name in self.connection.allowed_tables

    def _check_db_table_is_allowed(self) -> None:
        if self.db_name != self.connection.db_name or not self._is_allowed_table() and not self._is_allowed_subselect():
            raise exc.SourceDoesNotExist(db_message="", query="")

    def get_sql_source(self, alias: Optional[str] = None) -> Any:
        self._check_db_table_is_allowed()

        if self._is_allowed_subselect():
            return super(ClickHouseTemplatedSubselectDataSource, self).get_sql_source(alias)
        if self._is_allowed_table():
            return super(StandardSQLDataSource, self).get_sql_source(alias)
        raise exc.SourceDoesNotExist(db_message="", query="")

    @property
    def spec(self) -> StandardSQLDataSourceSpec:
        assert isinstance(self._spec, StandardSQLDataSourceSpec)
        return self._spec

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_FROZEN_SUBSELECT,
            SOURCE_TYPE_CH_FROZEN_SOURCE,
        }


class ClickHouseFrozenSubselectDataSourceBase(SubselectDataSource):
    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_FROZEN_SUBSELECT,
            SOURCE_TYPE_CH_FROZEN_SOURCE,
        }

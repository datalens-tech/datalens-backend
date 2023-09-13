from __future__ import annotations

from typing import ClassVar

from bi_i18n.localizer_base import Localizer
from bi_core.us_connection_base import DataSourceTemplate

from bi_configs.connectors_settings import CHFrozenConnectorSettings
from bi_constants.enums import RawSQLLevel

from bi_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_TABLE,
    SOURCE_TYPE_CH_SUBSELECT,
)
from bi_connector_bundle_ch_filtered.base.core.us_connection import ConnectionCHFilteredHardcodedDataBase
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.constants import (
    SOURCE_TYPE_CH_FROZEN_SOURCE, SOURCE_TYPE_CH_FROZEN_SUBSELECT
)


class ConnectionClickhouseFrozenBase(ConnectionCHFilteredHardcodedDataBase[CHFrozenConnectorSettings]):
    source_type = SOURCE_TYPE_CH_FROZEN_SOURCE
    allowed_source_types = frozenset((
        SOURCE_TYPE_CH_TABLE,
        SOURCE_TYPE_CH_SUBSELECT,
        SOURCE_TYPE_CH_FROZEN_SUBSELECT
    ))
    allow_cache: ClassVar[bool] = True
    is_always_internal_source: ClassVar[bool] = True
    settings_type = CHFrozenConnectorSettings

    @property
    def pass_db_query_to_user(self) -> bool:
        return self._connector_settings.PASS_DB_QUERY_TO_USER

    @property
    def raw_sql_level(self) -> RawSQLLevel:
        # NOTE
        # This could be a class variable, but CreateDSFrom is strictly bound to the datasource class, which
        # in turn is bound to the connection class.
        # This means that we can't create a subclass of this class and continue using same CreateDSFrom –
        # they all should point to this class.
        # In other words, we can't inherit from this class to change frozen connectors' behaviour for now.
        return self._connector_settings.RAW_SQL_LEVEL

    @property
    def is_subselect_allowed(self) -> bool:
        return True

    @property
    def is_dashsql_allowed(self) -> bool:
        return self.raw_sql_level == RawSQLLevel.dashsql

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        if self.raw_sql_level == RawSQLLevel.off:
            return []
        return self._make_subselect_templates(source_type=SOURCE_TYPE_CH_FROZEN_SUBSELECT, localizer=localizer)

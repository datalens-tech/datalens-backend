from __future__ import annotations

from typing import ClassVar

from bi_i18n.localizer_base import Localizer
from bi_core.us_connection_base import DataSourceTemplate

from bi_configs.connectors_settings import CHFrozenConnectorSettings

from bi_constants.enums import CreateDSFrom, ConnectionType, RawSQLLevel

from bi_core.connectors.clickhouse_base.us_connection import ConnectionCHFilteredHardcodedDataBase
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.constants import (
    SOURCE_TYPE_CH_FROZEN_SOURCE, SOURCE_TYPE_CH_FROZEN_SUBSELECT
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS,
)
from bi_connector_bundle_ch_frozen.ch_frozen_covid.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_COVID,
)
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_DEMO,
)
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_DTP,
)
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_GKH,
)
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_HORECA,
)
from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_SAMPLES,
)
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY,
)
from bi_connector_bundle_ch_frozen.ch_frozen_weather.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_WEATHER,
)


class ConnectionClickhouseFrozenBase(ConnectionCHFilteredHardcodedDataBase):
    source_type = SOURCE_TYPE_CH_FROZEN_SOURCE
    allowed_source_types = frozenset((
        CreateDSFrom.CH_TABLE,
        CreateDSFrom.CH_SUBSELECT,
        SOURCE_TYPE_CH_FROZEN_SUBSELECT
    ))
    allow_cache: ClassVar[bool] = True
    is_always_internal_source: ClassVar[bool] = True

    FROZEN_CONNECTORS_SETTINGS_KEY_BY_CONN_TYPE: dict[ConnectionType, str] = {
        # FIXME: Connectorize: BI-4614
        CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS: 'CH_FROZEN_BUMPY_ROADS',
        CONNECTION_TYPE_CH_FROZEN_COVID: 'CH_FROZEN_COVID',
        CONNECTION_TYPE_CH_FROZEN_DEMO: 'CH_FROZEN_DEMO',
        CONNECTION_TYPE_CH_FROZEN_DTP: 'CH_FROZEN_DTP',
        CONNECTION_TYPE_CH_FROZEN_GKH: 'CH_FROZEN_GKH',
        CONNECTION_TYPE_CH_FROZEN_SAMPLES: 'CH_FROZEN_SAMPLES',
        CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY: 'CH_FROZEN_TRANSPARENCY',
        CONNECTION_TYPE_CH_FROZEN_WEATHER: 'CH_FROZEN_WEATHER',
        CONNECTION_TYPE_CH_FROZEN_HORECA: 'CH_FROZEN_HORECA',
    }

    @property
    def _connector_settings(self) -> CHFrozenConnectorSettings:
        connector_settings_key = self.FROZEN_CONNECTORS_SETTINGS_KEY_BY_CONN_TYPE[self.conn_type]
        settings = self._all_connectors_settings.__getattribute__(connector_settings_key)
        assert settings is not None
        return settings

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

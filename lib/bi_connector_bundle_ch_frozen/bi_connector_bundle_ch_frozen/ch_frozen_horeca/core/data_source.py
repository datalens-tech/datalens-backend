from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.data_source import ClickHouseFrozenDataSourceBase
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.constants import CONNECTION_TYPE_CH_FROZEN_HORECA


class ClickHouseFrozenHorecaDataSource(ClickHouseFrozenDataSourceBase):
    conn_type = CONNECTION_TYPE_CH_FROZEN_HORECA

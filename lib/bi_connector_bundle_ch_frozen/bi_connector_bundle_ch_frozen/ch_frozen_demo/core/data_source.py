from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.data_source import (
    ClickHouseFrozenDataSourceBase,
    ClickHouseFrozenSubselectDataSourceBase,
)
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.constants import CONNECTION_TYPE_CH_FROZEN_DEMO


class ClickHouseFrozenDemoDataSource(ClickHouseFrozenDataSourceBase):
    conn_type = CONNECTION_TYPE_CH_FROZEN_DEMO


class ClickHouseFrozenDemoSubselectDataSource(ClickHouseFrozenSubselectDataSourceBase):
    conn_type = CONNECTION_TYPE_CH_FROZEN_DEMO

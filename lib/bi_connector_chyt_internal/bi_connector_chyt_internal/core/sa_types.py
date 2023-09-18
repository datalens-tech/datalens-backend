from dl_connector_clickhouse.core.clickhouse_base.sa_types import (
    SQLALCHEMY_CLICKHOUSE_BASE_TYPES,
    _generate_complex_ch_types,
)

from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)

SQLALCHEMY_CHYT_INTERNAL_TYPES = _generate_complex_ch_types(
    SQLALCHEMY_CLICKHOUSE_BASE_TYPES,
    conn_type=CONNECTION_TYPE_CH_OVER_YT,
) | _generate_complex_ch_types(
    SQLALCHEMY_CLICKHOUSE_BASE_TYPES,
    conn_type=CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)

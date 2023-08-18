from bi_constants.enums import ConnectionType

from bi_core.connectors.clickhouse_base.sa_types import SQLALCHEMY_CLICKHOUSE_BASE_TYPES, _generate_complex_ch_types


SQLALCHEMY_CHYT_INTERNAL_TYPES = _generate_complex_ch_types(
    SQLALCHEMY_CLICKHOUSE_BASE_TYPES, conn_type=ConnectionType.ch_over_yt,
) | _generate_complex_ch_types(
    SQLALCHEMY_CLICKHOUSE_BASE_TYPES, conn_type=ConnectionType.ch_over_yt_user_auth,
)

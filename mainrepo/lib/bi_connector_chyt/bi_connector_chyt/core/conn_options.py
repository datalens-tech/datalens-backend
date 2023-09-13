from __future__ import annotations

import attr

from bi_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions


@attr.s(frozen=True, hash=True)
class CHYTConnectOptions(CHConnectOptions):
    pass

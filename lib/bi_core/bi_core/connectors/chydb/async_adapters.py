from __future__ import annotations

import attr

from clickhouse_sqlalchemy.drivers.http.transport import _get_type  # noqa

from bi_constants.enums import ConnectionType

from bi_core.connectors.clickhouse_base.adapters import BaseAsyncClickHouseAdapter
from bi_core.connectors.clickhouse_base.ch_commons import CHYDBUtils


@attr.s
class AsyncCHYDBAdapter(BaseAsyncClickHouseAdapter):
    conn_type = ConnectionType.chydb
    ch_utils = CHYDBUtils

from __future__ import annotations

import attr

from dl_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseAsyncAdapterConnExecutor,
    ClickHouseSyncAdapterConnExecutor,
)

from bi_connector_mdb_base.core.connection_executors import MDBHostConnExecutorMixin


@attr.s(cmp=False, hash=False)
class SyncClickHouseMDBConnExecutor(MDBHostConnExecutorMixin, ClickHouseSyncAdapterConnExecutor):
    pass


@attr.s(cmp=False, hash=False)
class AsyncClickHouseMDBConnExecutor(MDBHostConnExecutorMixin, ClickHouseAsyncAdapterConnExecutor):
    pass

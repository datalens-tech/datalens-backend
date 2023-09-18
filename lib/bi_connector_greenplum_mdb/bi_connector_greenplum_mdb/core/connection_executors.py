from __future__ import annotations

import attr

from dl_connector_postgresql.core.postgresql_base.connection_executors import (
    PostgresConnExecutor,
    AsyncPostgresConnExecutor,
)
from bi_connector_mdb_base.core.connection_executors import MDBHostConnExecutorMixin


@attr.s(cmp=False, hash=False)
class GreenplumMDBConnExecutor(MDBHostConnExecutorMixin, PostgresConnExecutor):
    pass


@attr.s(cmp=False, hash=False)
class AsyncGreenplumMDBConnExecutor(MDBHostConnExecutorMixin, AsyncPostgresConnExecutor):
    pass

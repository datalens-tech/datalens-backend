from __future__ import annotations

import attr

from bi_connector_mysql.core.connection_executors import MySQLConnExecutor, AsyncMySQLConnExecutor
from bi_connector_mdb_base.core.connection_executors import MDBHostConnExecutorMixin


@attr.s(cmp=False, hash=False)
class MySQLMDBConnExecutor(MDBHostConnExecutorMixin, MySQLConnExecutor):
    pass


@attr.s(cmp=False, hash=False)
class AsyncMySQLMDBConnExecutor(MDBHostConnExecutorMixin, AsyncMySQLConnExecutor):
    pass
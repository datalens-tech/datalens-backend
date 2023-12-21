from dl_connector_greenplum.core.adapters import (
    AsyncGreenplumAdapter,
    GreenplumAdapter,
)
from dl_connector_postgresql.core.postgresql_base.connection_executors import (
    AsyncPostgresConnExecutor,
    PostgresConnExecutor,
)


class GreenplumConnExecutor(PostgresConnExecutor):
    TARGET_ADAPTER_CLS = GreenplumAdapter


class AsyncGreenplumConnExecutor(AsyncPostgresConnExecutor):
    TARGET_ADAPTER_CLS = AsyncGreenplumAdapter

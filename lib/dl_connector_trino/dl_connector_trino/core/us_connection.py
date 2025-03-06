from typing import (
    Callable,
    ClassVar,
    Optional,
    cast,
)

import attr

from dl_core.connection_executors import SyncConnExecutorBase
from dl_core.connection_executors.sync_executor_wrapper import SyncWrapperForAsyncConnExecutor
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionSQL,
)
from dl_core.utils import secrepr

from dl_connector_trino.core.adapters import TrinoDefaultAdapter
from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    SOURCE_TYPE_TRINO_SUBSELECT,
    SOURCE_TYPE_TRINO_TABLE,
    TrinoAuthType,
)
from dl_connector_trino.core.dto import TrinoConnDTO


class ConnectionTrino(ConnectionSQL):
    conn_type = CONNECTION_TYPE_TRINO
    has_schema: ClassVar[bool] = True
    default_schema_name = None
    source_type = SOURCE_TYPE_TRINO_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_TRINO_TABLE, SOURCE_TYPE_TRINO_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ConnectionSQL.DataModel):
        auth_type: TrinoAuthType = attr.ib(default=TrinoAuthType.NONE)
        ssl_ca: Optional[str] = attr.ib(repr=secrepr, default=None)
        jwt: Optional[str] = attr.ib(repr=secrepr, default=None)

    def get_conn_dto(self) -> TrinoConnDTO:
        return TrinoConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            port=self.data.port,
            username=self.data.username,
            auth_type=self.data.auth_type,
            password=self.data.password,
            jwt=self.data.jwt,
            ssl_ca=self.data.ssl_ca,
        )

    def get_catalog_names(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[str]:
        # It is impossible to define `get_catalog_names` in `TrinoConnExecutor` and call it from here
        # because `conn_executor_factory` returns `SyncWrapperForAsyncConnExecutor`, not `TrinoConnExecutor` (surprise!).
        # So, `SyncWrapperForAsyncConnExecutor` sets a set of methods that can be defined in `TrinoConnExecutor`.
        # To keep source querying logic in `Adapter`, we need this hack with `_extract_sync_sa_adapter`.
        conn_executor = cast(SyncWrapperForAsyncConnExecutor, conn_executor_factory(self))
        sa_adapter = cast(TrinoDefaultAdapter, conn_executor._extract_sync_sa_adapter(raise_on_not_exists=True))
        return sa_adapter.get_catalog_names()

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        parameter_combinations: list[dict] = []
        catalog_names = self.get_catalog_names(conn_executor_factory=conn_executor_factory)
        for catalog_name in catalog_names:
            tables = self.get_tables(conn_executor_factory=conn_executor_factory, db_name=catalog_name)
            parameter_combinations.extend(
                dict(
                    db_name=catalog_name,
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                )
                for table in tables
            )

        return parameter_combinations

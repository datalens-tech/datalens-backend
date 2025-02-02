from typing import (
    Callable,
    ClassVar,
    Optional,
)

import attr

from dl_core.connection_executors import SyncConnExecutorBase
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionSQL,
)
from dl_core.utils import secrepr

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

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        parameter_combinations: list[dict] = []
        schemas = self.get_schema_names(conn_executor_factory=conn_executor_factory)
        if not schemas:
            return parameter_combinations

        for schema_name in schemas:
            for table in self.get_tables(conn_executor_factory=conn_executor_factory, db_name=schema_name):
                parameter_combinations.append(dict(db_name=schema_name, table_name=table.table_name))

        return parameter_combinations

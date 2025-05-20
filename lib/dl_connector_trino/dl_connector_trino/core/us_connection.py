from typing import (
    Callable,
    ClassVar,
    Optional,
)

import attr
import sqlalchemy as sa
from trino.sqlalchemy.dialect import TrinoDialect

from dl_core.connection_executors import SyncConnExecutorBase
from dl_core.connection_executors.common_base import ConnExecutorQuery
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


TRINO_SYSTEM_CATALOGS = (
    "system",
    "tpch",
    "tpcds",
    "jmx",
)

TRINO_CATALOGS = sa.Table(
    "catalogs",
    sa.MetaData(),
    sa.Column("table_cat", sa.String),
    schema="jdbc",
)
GET_TRINO_CATALOGS_QUERY = str(
    sa.select(
        TRINO_CATALOGS.c.table_cat,
    )
    .where(
        ~TRINO_CATALOGS.c.table_cat.in_(TRINO_SYSTEM_CATALOGS),
    )
    .order_by(
        TRINO_CATALOGS.c.table_cat,
    )
    .compile(dialect=TrinoDialect(), compile_kwargs={"literal_binds": True})
)


class ConnectionTrino(ConnectionSQL):
    conn_type = CONNECTION_TYPE_TRINO
    has_schema: ClassVar[bool] = True
    default_schema_name = None
    source_type = SOURCE_TYPE_TRINO_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_TRINO_TABLE, SOURCE_TYPE_TRINO_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    allow_export: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ConnectionSQL.DataModel):
        auth_type: TrinoAuthType = attr.ib()
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

    def get_catalogs(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[str]:
        conn_executor = conn_executor_factory(self)
        catalogs_query = ConnExecutorQuery(query=GET_TRINO_CATALOGS_QUERY, db_name="system")
        catalogs_res = conn_executor.execute(query=catalogs_query)
        return [catalog_row[0] for catalog_row in catalogs_res.get_all()]

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        parameter_combinations: list[dict] = []
        for catalog_name in self.get_catalogs(conn_executor_factory=conn_executor_factory):
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

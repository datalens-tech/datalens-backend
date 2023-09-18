import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL


@attr.s(frozen=True)
class MSSQLConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_MSSQL

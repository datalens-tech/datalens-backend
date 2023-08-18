import attr

from bi_core.connection_models.dto_defs import DefaultSQLDTO

from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL


@attr.s(frozen=True)
class MySQLConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_MYSQL

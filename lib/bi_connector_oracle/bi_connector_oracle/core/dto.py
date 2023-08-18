import attr

from bi_core.connection_models.dto_defs import DefaultSQLDTO

from bi_connector_oracle.core.constants import OracleDbNameType, CONNECTION_TYPE_ORACLE


@attr.s(frozen=True)
class OracleConnDTO(DefaultSQLDTO):  # noqa
    conn_type = CONNECTION_TYPE_ORACLE

    db_name_type: OracleDbNameType = attr.ib(kw_only=True)

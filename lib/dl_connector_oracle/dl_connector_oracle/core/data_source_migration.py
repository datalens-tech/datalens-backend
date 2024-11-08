from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator
from dl_core.data_source_spec.sql import StandardSchemaSQLDataSourceSpec

from dl_connector_oracle.core.constants import (
    SOURCE_TYPE_ORACLE_SUBSELECT,
    SOURCE_TYPE_ORACLE_TABLE,
)


class OracleDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_dsrc_spec_cls = StandardSchemaSQLDataSourceSpec
    table_source_type = SOURCE_TYPE_ORACLE_TABLE
    subselect_source_type = SOURCE_TYPE_ORACLE_SUBSELECT

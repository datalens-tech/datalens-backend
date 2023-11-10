from __future__ import annotations

import logging
from typing import (
    Any,
    Optional,
)

from dl_constants.enums import DataSourceType
from dl_core.data_source.sql import (
    BaseSQLDataSource,
    StandardSchemaSQLDataSource,
    SubselectDataSource,
    require_table_name,
)
from dl_core.utils import sa_plain_text

from dl_connector_oracle.core.constants import (
    CONNECTION_TYPE_ORACLE,
    SOURCE_TYPE_ORACLE_SUBSELECT,
    SOURCE_TYPE_ORACLE_TABLE,
)


LOGGER = logging.getLogger(__name__)


class OracleDataSourceMixin(BaseSQLDataSource):
    conn_type = CONNECTION_TYPE_ORACLE

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_ORACLE_TABLE, SOURCE_TYPE_ORACLE_SUBSELECT)


class OracleDataSource(OracleDataSourceMixin, StandardSchemaSQLDataSource):
    """Oracle table"""

    @require_table_name
    def get_sql_source(self, alias: Optional[str] = None) -> Any:
        q = self.quote
        alias_str = "" if alias is None else f" {q(alias)}"
        schema_str = "" if self.schema_name is None else f"{q(self.schema_name)}."
        return sa_plain_text(f"{schema_str}{q(self.table_name)}{alias_str}")


class OracleSubselectDataSource(OracleDataSourceMixin, SubselectDataSource):
    """Oracle subselect"""

    # In oracle, `(select …) as source` doesn't work, only `(select …) source`.
    _subquery_alias_joiner = " "

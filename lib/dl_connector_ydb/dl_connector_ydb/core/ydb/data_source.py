from dl_constants.enums import DataSourceType
from dl_core.data_source.sql import (
    StandardSQLDataSource,
    SubselectDataSource,
    require_table_name,
)
from dl_core.query.bi_query import SqlSourceType
from dl_core.utils import sa_plain_text

from dl_connector_ydb.core.base.data_source import YQLDataSourceMixin
from dl_connector_ydb.core.ydb.constants import (
    CONNECTION_TYPE_YDB,
    SOURCE_TYPE_YDB_SUBSELECT,
    SOURCE_TYPE_YDB_TABLE,
)


class YDBDataSourceMixin(YQLDataSourceMixin):
    conn_type = CONNECTION_TYPE_YDB

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_YDB_TABLE, SOURCE_TYPE_YDB_SUBSELECT)


class YDBTableDataSource(YDBDataSourceMixin, StandardSQLDataSource):
    """YDB table"""

    @require_table_name
    def get_sql_source(self, alias: str | None = None) -> SqlSourceType:
        # cross-db joins are not supported
        assert not self.db_name or self.db_name == self.connection.db_name  # type: ignore  # 2024-01-24 # TODO: "ConnectionBase" has no attribute "db_name"; maybe "dir_name"?  [attr-defined]

        # Unlike `super()`, not adding the database name here.
        table_name = self.table_name
        quoted_table = self.quote(table_name)

        if alias is None:
            return sa_plain_text(quoted_table)

        quoted_alias = self.quote(alias)
        return sa_plain_text(f"{quoted_table} AS {quoted_alias}")


class YDBSubselectDataSource(YDBDataSourceMixin, SubselectDataSource):
    """YDB subselect"""

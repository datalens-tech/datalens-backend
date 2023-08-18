from __future__ import annotations

from typing import Any

from bi_constants.enums import CreateDSFrom

from bi_connector_yql.core.yql_base.data_source import YQLDataSourceMixin
from bi_core.data_source.sql import StandardSQLDataSource, SubselectDataSource, require_table_name
from bi_core.utils import sa_plain_text

from bi_connector_yql.core.ydb.constants import (
    CONNECTION_TYPE_YDB, SOURCE_TYPE_YDB_TABLE, SOURCE_TYPE_YDB_SUBSELECT
)


class YDBDataSourceMixin(YQLDataSourceMixin):
    conn_type = CONNECTION_TYPE_YDB

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in (SOURCE_TYPE_YDB_TABLE, SOURCE_TYPE_YDB_SUBSELECT)


class YDBTableDataSource(YDBDataSourceMixin, StandardSQLDataSource):
    """ YDB table """

    @require_table_name
    def get_sql_source(self, alias: str = None) -> Any:
        # cross-db joins are not supported
        assert not self.db_name or self.db_name == self.connection.db_name

        # Unlike `super()`, not adding the database name here.
        q = self.quote
        alias_str = '' if alias is None else f' AS {q(alias)}'
        return sa_plain_text(f'{q(self.table_name)}{alias_str}')


class YDBSubselectDataSource(YDBDataSourceMixin, SubselectDataSource):
    """ YDB subselect """

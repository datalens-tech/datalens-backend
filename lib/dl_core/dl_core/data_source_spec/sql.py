from typing import (
    Any,
)

import attr

from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.db import IndexInfo
from dl_utils.utils import get_type_full_name


@attr.s
class SQLDataSourceSpecBase(DataSourceSpec):
    db_version: str | None = attr.ib(kw_only=True, default=None)


@attr.s
class SubselectDataSourceSpec(SQLDataSourceSpecBase):
    subsql: str | None = attr.ib(kw_only=True, default=None)


@attr.s
class DbSQLDataSourceSpec(SQLDataSourceSpecBase):
    db_name: str | None = attr.ib(kw_only=True, default=None)


@attr.s
class TableSQLDataSourceSpec(SQLDataSourceSpecBase):
    table_name: str | None = attr.ib(kw_only=True, default=None)

    @property
    def is_configured(self) -> bool:
        return self.table_name is not None


@attr.s
class IndexedSQLDataSourceSpec(SQLDataSourceSpecBase):
    index_info_set: frozenset[IndexInfo] | None = attr.ib(kw_only=True, default=None)

    @index_info_set.validator
    def check_indexes(self, attribute: Any, value: Any) -> None:
        if value is None or isinstance(value, frozenset):
            pass
        else:
            raise TypeError(f"Indexes must be a frozen set or non, not {get_type_full_name(type(value))!r}")


@attr.s
class StandardSQLDataSourceSpec(
    DbSQLDataSourceSpec,
    TableSQLDataSourceSpec,
    IndexedSQLDataSourceSpec,
):
    pass


@attr.s
class SchemaSQLDataSourceSpec(SQLDataSourceSpecBase):
    schema_name: str | None = attr.ib(kw_only=True, default=None)


@attr.s
class StandardSchemaSQLDataSourceSpec(
    StandardSQLDataSourceSpec,
    SchemaSQLDataSourceSpec,
):
    pass

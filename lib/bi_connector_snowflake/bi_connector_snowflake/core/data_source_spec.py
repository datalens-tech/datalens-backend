from __future__ import annotations

import attr

from bi_core.data_source_spec.sql import (
    TableSQLDataSourceSpec,
    SchemaSQLDataSourceSpec,
    SubselectDataSourceSpec,
    DbSQLDataSourceSpec,
)


@attr.s
class SnowFlakeTableDataSourceSpec(TableSQLDataSourceSpec, SchemaSQLDataSourceSpec, DbSQLDataSourceSpec):
    pass


@attr.s
class SnowFlakeSubselectDataSourceSpec(SubselectDataSourceSpec):
    pass

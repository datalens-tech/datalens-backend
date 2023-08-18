from __future__ import annotations

from typing import Optional

import attr

from bi_core.data_source_spec.sql import SQLDataSourceSpecBase, StandardSQLDataSourceSpec, SubselectDataSourceSpec


class CHYDBDataSourceSpecMixin(SQLDataSourceSpecBase):
    pass


@attr.s
class CHYDBTableDataSourceSpec(CHYDBDataSourceSpecMixin, StandardSQLDataSourceSpec):
    # Very special YDB terms that, in this data source, go into the
    # `sql_source`, not in the connection options.
    ydb_cluster: Optional[str] = attr.ib(kw_only=True, default=None)
    ydb_database: Optional[str] = attr.ib(kw_only=True, default=None)


class CHYDBSubselectDataSourceSpec(CHYDBDataSourceSpecMixin, SubselectDataSourceSpec):
    pass

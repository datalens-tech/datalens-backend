from __future__ import annotations

from typing import ClassVar

from dl_core.connectors.base.query_compiler import (
    QueryCompiler,
    SectionAliasMode,
)


class PostgreSQLQueryCompiler(QueryCompiler):
    force_nulls_lower_than_values = True

    groupby_alias_mode: ClassVar[SectionAliasMode] = SectionAliasMode.by_alias_in_section

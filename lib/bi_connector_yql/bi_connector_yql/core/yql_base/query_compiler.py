from __future__ import annotations

from typing import ClassVar

from dl_core.connectors.base.query_compiler import (
    QueryCompiler,
    SectionAliasMode,
)


class YQLQueryCompiler(QueryCompiler):
    groupby_alias_mode: ClassVar[SectionAliasMode] = SectionAliasMode.by_alias_in_select

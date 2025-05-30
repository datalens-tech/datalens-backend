from typing import ClassVar

from dl_core.connectors.base.query_compiler import (
    QueryCompiler,
    SectionAliasMode,
)


class TrinoQueryCompiler(QueryCompiler):
    groupby_alias_mode: ClassVar[SectionAliasMode] = SectionAliasMode.by_position_in_section

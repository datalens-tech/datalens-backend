from typing import ClassVar

from bi_core.connectors.base.query_compiler import QueryCompiler, SectionAliasMode


class MySQLQueryCompiler(QueryCompiler):
    groupby_alias_mode: ClassVar[SectionAliasMode] = SectionAliasMode.by_alias_in_section

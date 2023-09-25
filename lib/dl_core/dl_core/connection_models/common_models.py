from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Optional,
)

import attr
import sqlalchemy as sa

from dl_core.enums import TableDefinitionType


@attr.s(frozen=True, auto_attribs=True)
class DBIdent:
    db_name: Optional[str]

    def __str__(self) -> str:
        return self.db_name or ""


@attr.s(frozen=True, auto_attribs=True)
class SchemaIdent:
    db_name: Optional[str]
    schema_name: Optional[str]

    def __str__(self) -> str:
        return f"{self.db_name or ''}.{self.schema_name or ''}"

    def clone(self, **kwargs: Any) -> SchemaIdent:
        return attr.evolve(self, **kwargs)


class TableDefinition:
    def_type: ClassVar[TableDefinitionType]


@attr.s(frozen=True, auto_attribs=True)
class TableIdent(TableDefinition):
    def_type = TableDefinitionType.table_ident

    db_name: Optional[str]
    schema_name: Optional[str]
    table_name: str

    def __str__(self) -> str:
        return f"{self.db_name or ''}.{self.schema_name or ''}.{self.table_name or ''}"

    def clone(self, **kwargs: Any) -> TableIdent:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True, auto_attribs=True)
class SATextTableDefinition(TableDefinition):
    def_type = TableDefinitionType.sa_text

    text: sa.sql.elements.TextClause

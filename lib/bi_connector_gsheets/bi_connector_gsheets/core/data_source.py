from __future__ import annotations

from typing import Any, ClassVar, Optional

import sqlalchemy as sa

from bi_constants.enums import CreateDSFrom, ConnectionType, JoinType

from bi_core.connection_models import TableDefinition, TableIdent
from bi_core.data_source.sql import BaseSQLDataSource

from bi_connector_gsheets.core.constants import (
    CONNECTION_TYPE_GSHEETS, SOURCE_TYPE_GSHEETS,
)


class GSheetsDataSource(BaseSQLDataSource):
    supported_join_types: ClassVar[frozenset[JoinType]] = frozenset()

    conn_type = CONNECTION_TYPE_GSHEETS

    @property
    def db_version(self) -> Optional[str]:
        return None

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type == SOURCE_TYPE_GSHEETS

    def get_sql_source(self, alias: Optional[str] = None) -> Any:
        return sa.text('()')  # placeholder, should be cut in the dialect.

    def get_table_definition(self) -> TableDefinition:
        return TableIdent(db_name=None, schema_name=None, table_name='')

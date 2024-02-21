from __future__ import annotations

import os
from typing import (
    ClassVar,
    Optional,
)

from dl_constants.enums import (
    DataSourceType,
    JoinType,
)
from dl_core.connection_models import (
    TableDefinition,
    TableIdent,
)
from dl_core.data_source.sql import PseudoSQLDataSource

from dl_connector_bitrix_gds.core.constants import (
    CONNECTION_TYPE_BITRIX24,
    SOURCE_TYPE_BITRIX_GDS,
)


def _get_supported_join_types() -> frozenset[JoinType]:
    flag = os.environ.get("EXPERIMENTAL_BITRIX_ENABLE_JOIN_TYPES", "false")
    if flag.lower() == "true":
        return frozenset(
            {
                JoinType.inner,
                JoinType.left,
                JoinType.full,
                JoinType.right,
            }
        )

    return frozenset()


class BitrixGDSDataSource(PseudoSQLDataSource):
    supported_join_types: ClassVar[frozenset[JoinType]] = _get_supported_join_types()
    conn_type = CONNECTION_TYPE_BITRIX24

    @property
    def db_version(self) -> Optional[str]:
        return None

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type == SOURCE_TYPE_BITRIX_GDS

    def get_table_definition(self) -> TableDefinition:
        assert self.table_name is not None
        return TableIdent(db_name=self.db_name, schema_name=None, table_name=self.table_name)

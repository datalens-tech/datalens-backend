from __future__ import annotations

from typing import (
    Callable,
    ClassVar,
)

import attr

from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    ConnectionBase,
    DataSourceTemplate,
)
from dl_i18n.localizer_base import Localizer

from bi_connector_oracle.core.constants import (
    SOURCE_TYPE_ORACLE_SUBSELECT,
    SOURCE_TYPE_ORACLE_TABLE,
    OracleDbNameType,
)
from bi_connector_oracle.core.dto import OracleConnDTO


class ConnectionSQLOracle(ClassicConnectionSQL):
    has_schema: ClassVar[bool] = True
    # Default schema is usually defined on a per-user basis,
    # so it's better to omit the schema if it isn't explicitly specified.
    default_schema_name = None
    source_type = SOURCE_TYPE_ORACLE_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_ORACLE_TABLE, SOURCE_TYPE_ORACLE_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        db_name_type: OracleDbNameType = attr.ib(default=OracleDbNameType.service_name)

    def get_conn_dto(self) -> OracleConnDTO:
        return OracleConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),  # type: ignore  # TODO: fix
            port=self.data.port,
            db_name=self.data.db_name,
            db_name_type=self.data.db_name_type,
            username=self.data.username,
            password=self.password,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(source_type=SOURCE_TYPE_ORACLE_SUBSELECT, localizer=localizer)

    def get_parameter_combinations(
        self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase]
    ) -> list[dict]:
        if not self.db_name:
            return []

        assert self.has_schema
        return [
            dict(schema_name=tid.schema_name, table_name=tid.table_name)
            for tid in self.get_tables(schema_name=None, conn_executor_factory=conn_executor_factory)
        ]

    @property
    def allow_public_usage(self) -> bool:
        return True

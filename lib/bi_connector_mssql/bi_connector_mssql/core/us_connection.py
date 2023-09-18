from __future__ import annotations

from typing import Callable, ClassVar

import attr

from dl_i18n.localizer_base import Localizer
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.us_connection_base import ConnectionBase, ClassicConnectionSQL, DataSourceTemplate

from bi_connector_mssql.core.constants import SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_MSSQL_SUBSELECT
from bi_connector_mssql.core.dto import MSSQLConnDTO


class ConnectionMSSQL(ClassicConnectionSQL):
    has_schema: ClassVar[bool] = True
    default_schema_name = 'dbo'
    source_type = SOURCE_TYPE_MSSQL_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_MSSQL_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        pass

    def get_conn_dto(self) -> MSSQLConnDTO:
        return MSSQLConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),  # type: ignore  # TODO: fix
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(
            source_type=SOURCE_TYPE_MSSQL_SUBSELECT,
            field_doc_key='MSSQL_SUBSELECT/subsql',
            localizer=localizer,
        )

    def get_parameter_combinations(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
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

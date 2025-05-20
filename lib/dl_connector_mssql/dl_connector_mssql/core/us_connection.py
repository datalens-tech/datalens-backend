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
    ConnectionSettingsMixin,
    DataSourceTemplate,
    make_subselect_datasource_template,
    make_table_datasource_template,
)
from dl_i18n.localizer_base import Localizer

from dl_connector_mssql.core.constants import (
    SOURCE_TYPE_MSSQL_SUBSELECT,
    SOURCE_TYPE_MSSQL_TABLE,
)
from dl_connector_mssql.core.dto import MSSQLConnDTO
from dl_connector_mssql.core.settings import MSSQLConnectorSettings


class ConnectionMSSQL(
    ConnectionSettingsMixin[MSSQLConnectorSettings],
    ClassicConnectionSQL,
):
    has_schema: ClassVar[bool] = True
    default_schema_name = "dbo"
    source_type = SOURCE_TYPE_MSSQL_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_MSSQL_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    allow_export: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    settings_type = MSSQLConnectorSettings

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        pass

    def get_conn_dto(self) -> MSSQLConnDTO:
        return MSSQLConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,  # type: ignore  # 2024-01-24 # TODO: Argument "password" to "MSSQLConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            make_table_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_MSSQL_TABLE,
                localizer=localizer,
                disabled=False,
                template_enabled=self.is_datasource_template_allowed,
            ),
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_MSSQL_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                field_doc_key="MSSQL_SUBSELECT/subsql",
                template_enabled=self.is_datasource_template_allowed,
            ),
        ]

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
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

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._connector_settings.ENABLE_DATASOURCE_TEMPLATE and super().is_datasource_template_allowed

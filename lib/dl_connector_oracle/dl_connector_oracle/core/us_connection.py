from __future__ import annotations

from typing import (
    Callable,
    ClassVar,
    Optional,
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

from dl_connector_oracle.core.constants import (
    SOURCE_TYPE_ORACLE_SUBSELECT,
    SOURCE_TYPE_ORACLE_TABLE,
    OracleDbNameType,
)
from dl_connector_oracle.core.dto import OracleConnDTO
from dl_connector_oracle.core.settings import OracleConnectorSettings


class ConnectionSQLOracle(
    ConnectionSettingsMixin[OracleConnectorSettings],
    ClassicConnectionSQL,
):
    has_schema: ClassVar[bool] = True
    # Default schema is usually defined on a per-user basis,
    # so it's better to omit the schema if it isn't explicitly specified.
    default_schema_name = None
    source_type = SOURCE_TYPE_ORACLE_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_ORACLE_TABLE, SOURCE_TYPE_ORACLE_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    settings_type = OracleConnectorSettings

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        db_name_type: OracleDbNameType = attr.ib(default=OracleDbNameType.service_name)
        ssl_enable: bool = attr.ib(kw_only=True, default=False)
        ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    def get_conn_dto(self) -> OracleConnDTO:
        return OracleConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),
            port=self.data.port,
            db_name=self.data.db_name,
            db_name_type=self.data.db_name_type,
            username=self.data.username,
            password=self.password,  # type: ignore  # 2024-01-24 # TODO: Argument "password" to "OracleConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
            ssl_enable=self.data.ssl_enable,
            ssl_ca=self.data.ssl_ca,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            make_table_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_ORACLE_TABLE,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                template_enabled=self.is_datasource_template_allowed,
            ),
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_ORACLE_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                template_enabled=self.is_datasource_template_allowed,
            ),
        ]

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

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._connector_settings.ENABLE_DATASOURCE_TEMPLATE and super().is_datasource_template_allowed

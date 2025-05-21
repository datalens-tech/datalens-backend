from __future__ import annotations

from typing import ClassVar

from dl_core.us_connection_base import (
    ConnectionSettingsMixin,
    DataSourceTemplate,
    make_subselect_datasource_template,
    make_table_datasource_template,
)
from dl_i18n.localizer_base import Localizer

from dl_connector_greenplum.core.constants import (
    SOURCE_TYPE_GP_SUBSELECT,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_greenplum.core.dto import GreenplumConnDTO
from dl_connector_greenplum.core.settings import GreenplumConnectorSettings
from dl_connector_postgresql.core.postgresql_base.us_connection import ConnectionPostgreSQLBase


class GreenplumConnection(
    ConnectionSettingsMixin[GreenplumConnectorSettings],
    ConnectionPostgreSQLBase,
):
    source_type = SOURCE_TYPE_GP_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_GP_TABLE, SOURCE_TYPE_GP_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    settings_type = GreenplumConnectorSettings

    def get_conn_dto(self) -> GreenplumConnDTO:
        return GreenplumConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,  # type: ignore  # 2024-01-24 # TODO: Argument "password" to "GreenplumConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
            enforce_collate=self.data.enforce_collate,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            make_table_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_GP_TABLE,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                template_enabled=self.is_datasource_template_allowed,
            ),
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_GP_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                field_doc_key="PG_SUBSELECT/subsql",  # shared, currently.
                template_enabled=self.is_datasource_template_allowed,
            ),
        ]

    @property
    def allow_public_usage(self) -> bool:
        return True

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._connector_settings.ENABLE_DATASOURCE_TEMPLATE and super().is_datasource_template_allowed

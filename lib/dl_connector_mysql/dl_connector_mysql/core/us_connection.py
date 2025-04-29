from __future__ import annotations

from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    ConnectionSettingsMixin,
    DataSourceTemplate,
    make_subselect_datasource_template,
)
from dl_i18n.localizer_base import Localizer

from dl_connector_mysql.core.constants import (
    SOURCE_TYPE_MYSQL_SUBSELECT,
    SOURCE_TYPE_MYSQL_TABLE,
)
from dl_connector_mysql.core.dto import MySQLConnDTO
from dl_connector_mysql.core.settings import MySQLConnectorSettings


class ConnectionMySQL(
    ConnectionSettingsMixin[MySQLConnectorSettings],
    ClassicConnectionSQL,
):
    source_type = SOURCE_TYPE_MYSQL_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    allow_export: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    settings_type = MySQLConnectorSettings

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        ssl_enable: bool = attr.ib(kw_only=True, default=False)
        ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    def get_conn_dto(self) -> MySQLConnDTO:
        return MySQLConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,  # type: ignore  # 2024-01-24 # TODO: Argument "password" to "MySQLConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
            ssl_enable=self.data.ssl_enable,
            ssl_ca=self.data.ssl_ca,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_MYSQL_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                template_enabled=self.is_datasource_template_allowed,
            )
        ]

    @property
    def allow_public_usage(self) -> bool:
        return True

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._connector_settings.ENABLE_DATASOURCE_TEMPLATE and super().is_datasource_template_allowed

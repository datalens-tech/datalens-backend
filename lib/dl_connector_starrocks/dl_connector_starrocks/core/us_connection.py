from __future__ import annotations

from typing import ClassVar

import attr

from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    ConnectionSettingsMixin,
    DataSourceTemplate,
    make_subselect_datasource_template,
    make_table_datasource_template,
)
from dl_i18n.localizer_base import Localizer

from dl_connector_starrocks.core.constants import (
    SOURCE_TYPE_STARROCKS_SUBSELECT,
    SOURCE_TYPE_STARROCKS_TABLE,
)
from dl_connector_starrocks.core.dto import StarRocksConnDTO
from dl_connector_starrocks.core.settings import DeprecatedStarRocksConnectorSettings


class ConnectionStarRocks(
    ConnectionSettingsMixin[DeprecatedStarRocksConnectorSettings],
    ClassicConnectionSQL,
):
    source_type = SOURCE_TYPE_STARROCKS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_STARROCKS_TABLE, SOURCE_TYPE_STARROCKS_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    settings_type = DeprecatedStarRocksConnectorSettings

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        pass

    def get_conn_dto(self) -> StarRocksConnDTO:
        return StarRocksConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,  # type: ignore  # 2024-01-24 # TODO: Argument "password" to "StarRocksConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        result: list[DataSourceTemplate] = []

        if self._connector_settings.ENABLE_TABLE_DATASOURCE_FORM:
            result.append(
                make_table_datasource_template(
                    connection_id=self.uuid,  # type: ignore
                    source_type=SOURCE_TYPE_STARROCKS_TABLE,
                    localizer=localizer,
                    disabled=not self.is_subselect_allowed,
                    template_enabled=self.is_datasource_template_allowed,
                )
            )

        result.append(
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_STARROCKS_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                template_enabled=self.is_datasource_template_allowed,
            )
        )

        return result

    @property
    def allow_public_usage(self) -> bool:
        return True

    @property
    def is_subselect_allowed(self) -> bool:
        return True

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._connector_settings.ENABLE_DATASOURCE_TEMPLATE and super().is_datasource_template_allowed

from __future__ import annotations

from typing import ClassVar

import attr

from dl_constants.enums import DashSQLQueryType
from dl_core.us_connection_base import (
    ConnectionSettingsMixin,
    DataSourceTemplate,
    QueryTypeInfo,
    make_subselect_datasource_template,
    make_table_datasource_template,
)
from dl_i18n.localizer_base import Localizer

from dl_connector_clickhouse.core.clickhouse.constants import (
    DEFAULT_CLICKHOUSE_USER,
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_connector_clickhouse.core.clickhouse.dto import DLClickHouseConnDTO
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse.core.clickhouse_base.us_connection import ConnectionClickhouseBase


class ConnectionClickhouse(
    ConnectionSettingsMixin[ClickHouseConnectorSettings],
    ConnectionClickhouseBase,
):
    """
    User's ClickHouse database.
    Should not be used for internal clickhouses.
    """

    class DataModel(ConnectionClickhouseBase.DataModel):
        readonly: int = attr.ib(kw_only=True, default=2)

    source_type = SOURCE_TYPE_CH_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_TABLE, SOURCE_TYPE_CH_SUBSELECT))
    settings_type = ClickHouseConnectorSettings

    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = False  # TODO: should be `True`, but need some cleanup for that.

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            make_table_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_CH_TABLE,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                template_enabled=self.is_datasource_template_allowed,
            ),
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_CH_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                template_enabled=self.is_datasource_template_allowed,
            ),
        ]

    def get_conn_dto(self) -> DLClickHouseConnDTO:
        base_dto = super().get_conn_dto()
        return DLClickHouseConnDTO(
            conn_id=self.uuid,
            protocol="https" if self.data.secure else "http",
            host=self.data.host,
            multihosts=self.parse_multihosts(),
            port=self.data.port,
            endpoint=self.data.endpoint,
            cluster_name=self.data.cluster_name,
            db_name=self.data.db_name,
            username=base_dto.username or DEFAULT_CLICKHOUSE_USER,
            password=base_dto.password or "",
            secure=self.data.secure,
            ssl_ca=self.data.ssl_ca,
            readonly=self.data.readonly,
        )

    @property
    def allow_public_usage(self) -> bool:
        return True

    def get_supported_query_type_infos(self) -> frozenset[QueryTypeInfo]:
        return frozenset(
            {
                QueryTypeInfo(
                    query_type=DashSQLQueryType.generic_query,
                    allow_selector=True,
                ),
            }
        )

    @property
    def is_typed_query_allowed(self) -> bool:
        return True

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._connector_settings.ENABLE_DATASOURCE_TEMPLATE and super().is_datasource_template_allowed

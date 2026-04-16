from typing import (
    Callable,
    ClassVar,
)

import attr

from dl_core.connection_executors import SyncConnExecutorBase
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionSettingsMixin,
    ConnectionSQL,
    DataSourceTemplate,
    make_subselect_datasource_template,
    make_table_datasource_template,
    parse_comma_separated_hosts,
)
from dl_i18n.localizer_base import Localizer

from dl_connector_starrocks.api.i18n.localizer import Translatable
from dl_connector_starrocks.core.constants import (
    CONNECTION_TYPE_STARROCKS,
    GET_STARROCKS_CATALOGS_QUERY,
    SOURCE_TYPE_STARROCKS_SUBSELECT,
    SOURCE_TYPE_STARROCKS_TABLE,
    STARROCKS_SYSTEM_CATALOGS,
    ListingSources,
)
from dl_connector_starrocks.core.dto import StarRocksConnDTO
from dl_connector_starrocks.core.settings import StarRocksConnectorSettings


class ConnectionStarRocks(
    ConnectionSettingsMixin[StarRocksConnectorSettings],
    ConnectionSQL,
):
    conn_type = CONNECTION_TYPE_STARROCKS
    has_schema: ClassVar[bool] = True
    default_schema_name = None
    source_type = SOURCE_TYPE_STARROCKS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_STARROCKS_TABLE, SOURCE_TYPE_STARROCKS_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    settings_type = StarRocksConnectorSettings
    supports_source_search = True
    supports_source_pagination = True
    db_name_required_for_search = True
    supports_db_name_listing = True

    @attr.s(kw_only=True)
    class DataModel(ConnectionSQL.DataModel):
        listing_sources: ListingSources = attr.ib(default=ListingSources.on)
        ssl_enable: bool = attr.ib(default=False)
        ssl_ca: str | None = attr.ib(default=None)

    @property
    def allow_public_usage(self) -> bool:
        return True

    @property
    def password(self) -> str | None:
        return self.data.password

    def parse_multihosts(self) -> tuple[str, ...]:
        return parse_comma_separated_hosts(self.data.host)

    @property
    def is_subselect_allowed(self) -> bool:
        return True

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._connector_settings.ENABLE_DATASOURCE_TEMPLATE and super().is_datasource_template_allowed

    def get_conn_dto(self) -> StarRocksConnDTO:
        return StarRocksConnDTO(
            conn_id=self.uuid or "",
            host=self.data.host,
            multihosts=self.parse_multihosts(),
            port=self.data.port,
            username=self.data.username,
            password=self.password or "",
            ssl_enable=self.data.ssl_enable,
            ssl_ca=self.data.ssl_ca,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        result: list[DataSourceTemplate] = []

        if self._connector_settings.ENABLE_TABLE_DATASOURCE_FORM:
            result.append(
                make_table_datasource_template(
                    connection_id=self.uuid or "",
                    source_type=SOURCE_TYPE_STARROCKS_TABLE,
                    localizer=localizer,
                    disabled=not self.is_subselect_allowed,
                    template_enabled=self.is_datasource_template_allowed,
                    db_name_form_enabled=True,
                    db_name_form_title=localizer.translate(Translatable("source_templates-label-starrocks_catalog")),
                    schema_name_form_enabled=True,
                )
            )

        result.append(
            make_subselect_datasource_template(
                connection_id=self.uuid or "",
                source_type=SOURCE_TYPE_STARROCKS_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                template_enabled=self.is_datasource_template_allowed,
            )
        )

        return result

    def get_db_names(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[str]:
        conn_executor = conn_executor_factory(self)
        catalogs_query = ConnExecutorQuery(query=GET_STARROCKS_CATALOGS_QUERY, db_name="")
        catalogs_res = conn_executor.execute(query=catalogs_query)
        return [
            catalog_row[0] for catalog_row in catalogs_res.get_all() if catalog_row[0] not in STARROCKS_SYSTEM_CATALOGS
        ]

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
        search_text: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        db_name: str | None = None,
    ) -> list[dict]:
        parameter_combinations: list[dict] = []

        if self.data.listing_sources is ListingSources.off:
            return parameter_combinations

        if db_name:
            tables = self.get_tables(
                conn_executor_factory=conn_executor_factory,
                db_name=db_name,
                search_text=search_text,
                limit=limit,
                offset=offset,
            )
            return [
                dict(
                    db_name=db_name,
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                )
                for table in tables
            ]

        # List tables from all available catalogs
        catalog_names = self.get_db_names(conn_executor_factory=conn_executor_factory)

        for catalog_name in catalog_names:
            tables = self.get_tables(
                conn_executor_factory=conn_executor_factory,
                db_name=catalog_name,
            )
            parameter_combinations.extend(
                dict(
                    db_name=catalog_name,
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                )
                for table in tables
            )

        return parameter_combinations

    @classmethod
    def get_db_name_label(cls, localizer: Localizer) -> str | None:
        return localizer.translate(Translatable("db_name-label"))

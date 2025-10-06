from typing import (
    Callable,
    ClassVar,
)

import attr
import sqlalchemy as sa
from trino.sqlalchemy.dialect import TrinoDialect

from dl_core.connection_executors import SyncConnExecutorBase
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionSettingsMixin,
    ConnectionSQL,
    DataSourceTemplate,
    make_subselect_datasource_template,
    make_table_datasource_template,
)
from dl_core.utils import secrepr
from dl_i18n.localizer_base import Localizer

from dl_connector_trino.api.i18n.localizer import Translatable
from dl_connector_trino.core.conn_options import TrinoConnectOptions
from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    SOURCE_TYPE_TRINO_SUBSELECT,
    SOURCE_TYPE_TRINO_TABLE,
    ListingSources,
    TrinoAuthType,
)
from dl_connector_trino.core.dto import TrinoConnDTO
from dl_connector_trino.core.settings import TrinoConnectorSettings


TRINO_SYSTEM_CATALOGS = (
    "system",
    "tpch",
    "tpcds",
    "jmx",
)

TRINO_CATALOGS = sa.Table(
    "catalogs",
    sa.MetaData(),
    sa.Column("table_cat", sa.String),
    schema="jdbc",
)
GET_TRINO_CATALOGS_QUERY = str(
    sa.select(
        TRINO_CATALOGS.c.table_cat,
    )
    .where(
        ~TRINO_CATALOGS.c.table_cat.in_(TRINO_SYSTEM_CATALOGS),
    )
    .order_by(
        TRINO_CATALOGS.c.table_cat,
    )
    .compile(dialect=TrinoDialect(), compile_kwargs={"literal_binds": True})
)


class ConnectionTrinoBase(
    ConnectionSettingsMixin[TrinoConnectorSettings],
    ConnectionSQL,
):
    conn_type = CONNECTION_TYPE_TRINO
    has_schema: ClassVar[bool] = True
    default_schema_name = None
    source_type = SOURCE_TYPE_TRINO_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_TRINO_TABLE, SOURCE_TYPE_TRINO_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    settings_type = TrinoConnectorSettings
    supports_source_search = True
    supports_source_pagination = True
    db_name_required_for_search = True
    supports_db_name_listing = True

    @attr.s(kw_only=True)
    class DataModel(ConnectionSQL.DataModel):
        auth_type: TrinoAuthType | None = attr.ib(default=None)
        ssl_enable: bool = attr.ib(default=False)
        ssl_ca: str | None = attr.ib(default=None)
        jwt: str | None = attr.ib(repr=secrepr, default=None)
        listing_sources: ListingSources = attr.ib()

    def get_conn_options(self) -> TrinoConnectOptions:
        base = super().get_conn_options()
        return base.to_subclass(
            TrinoConnectOptions,
            connect_timeout=1,
            total_timeout=80,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        result: list[DataSourceTemplate] = []

        if self._connector_settings.ENABLE_TABLE_DATASOURCE_FORM:
            result.append(
                make_table_datasource_template(
                    connection_id=self.uuid,  # type: ignore
                    source_type=SOURCE_TYPE_TRINO_TABLE,
                    localizer=localizer,
                    disabled=not self.is_subselect_allowed,
                    template_enabled=False,  # TODO BI-6411 enable dsrc templating
                    db_name_form_enabled=True,
                    db_name_form_title=localizer.translate(Translatable("source_templates-label-trino_catalog")),
                    schema_name_form_enabled=True,
                )
            )

        result.append(
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_TRINO_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                title="Subselect over Trino",
            )
        )

        return result

    def get_db_names(  # db names - catalogs in the case of trino
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[str]:
        conn_executor = conn_executor_factory(self)
        catalogs_query = ConnExecutorQuery(query=GET_TRINO_CATALOGS_QUERY, db_name="system")
        catalogs_res = conn_executor.execute(query=catalogs_query)
        return [catalog_row[0] for catalog_row in catalogs_res.get_all()]

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
            # List tables only from the specified catalog
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

        # Get tables from each catalog - listing is not supported in this case
        # Here we assume that the parameters are already validated by
        # validate_source_listing_parameters method of the base class
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


class ConnectionTrino(ConnectionTrinoBase):
    @attr.s(kw_only=True)
    class DataModel(ConnectionTrinoBase.DataModel):
        auth_type: TrinoAuthType = attr.ib()

    def get_conn_dto(self) -> TrinoConnDTO:
        return TrinoConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            port=self.data.port,
            username=self.data.username,
            auth_type=self.data.auth_type,
            password=self.data.password,
            jwt=self.data.jwt,
            ssl_enable=self.data.ssl_enable,
            ssl_ca=self.data.ssl_ca,
        )

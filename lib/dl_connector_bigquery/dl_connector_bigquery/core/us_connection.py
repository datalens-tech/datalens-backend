from __future__ import annotations

from typing import (
    Callable,
    ClassVar,
)

import attr

from dl_core.base_models import (
    ConnCacheableDataModelMixin,
    ConnectionDataModelBase,
    ConnRawSqlLevelDataModelMixin,
)
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionSQL,
    DataSourceTemplate,
    make_subselect_datasource_template,
)
from dl_i18n.localizer_base import Localizer
from dl_utils.utils import DataKey

from dl_connector_bigquery.core.constants import (
    CONNECTION_TYPE_BIGQUERY,
    SOURCE_TYPE_BIGQUERY_SUBSELECT,
    SOURCE_TYPE_BIGQUERY_TABLE,
)
from dl_connector_bigquery.core.dto import BigQueryConnDTO


class ConnectionSQLBigQuery(ConnectionSQL):
    conn_type = CONNECTION_TYPE_BIGQUERY

    has_schema: ClassVar[bool] = True
    default_schema_name = None
    source_type = SOURCE_TYPE_BIGQUERY_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_BIGQUERY_TABLE, SOURCE_TYPE_BIGQUERY_SUBSELECT))
    allow_dashsql: ClassVar[bool] = False
    allow_cache: ClassVar[bool] = True
    allow_export: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s
    class DataModel(
        ConnCacheableDataModelMixin,
        ConnRawSqlLevelDataModelMixin,
        ConnectionDataModelBase,
    ):
        credentials: str = attr.ib(kw_only=True)
        project_id: str = attr.ib(kw_only=True)
        data_export_forbidden: bool = attr.ib(default=False)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {DataKey(parts=("credentials",))}

    @property
    def project_id(self) -> str:
        return self.data.project_id

    def get_conn_dto(self) -> BigQueryConnDTO:
        return BigQueryConnDTO(
            conn_id=self.uuid,
            project_id=self.data.project_id,
            credentials=self.data.credentials,
        )

    def get_data_source_template_group(self, parameters: dict) -> list[str]:
        return [val for val in (parameters.get("project_id"), parameters.get("dataset_name")) if val is not None]

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        tables = self.get_tables(conn_executor_factory)
        return [
            dict(
                project_id=table_ident.db_name,
                dataset_name=table_ident.schema_name,
                table_name=table_ident.table_name,
            )
            for table_ident in tables
        ]

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_BIGQUERY_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
            )
        ]

    @property
    def allow_public_usage(self) -> bool:
        return False

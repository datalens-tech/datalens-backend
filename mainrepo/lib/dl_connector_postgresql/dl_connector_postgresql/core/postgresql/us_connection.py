from __future__ import annotations

from typing import ClassVar

from dl_core.us_connection_base import DataSourceTemplate
from dl_i18n.localizer_base import Localizer

from dl_connector_postgresql.core.postgresql.constants import (
    SOURCE_TYPE_PG_SUBSELECT,
    SOURCE_TYPE_PG_TABLE,
)
from dl_connector_postgresql.core.postgresql.dto import PostgresConnDTO
from dl_connector_postgresql.core.postgresql_base.us_connection import ConnectionPostgreSQLBase


class ConnectionPostgreSQL(ConnectionPostgreSQLBase):
    source_type = SOURCE_TYPE_PG_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_PG_TABLE, SOURCE_TYPE_PG_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    def get_conn_dto(self) -> PostgresConnDTO:
        multihosts = self.parse_multihosts()
        enforce_collate = self._get_effective_enforce_collate(
            enforce_collate=self.data.enforce_collate,
            multihosts=multihosts,
        )
        return PostgresConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=multihosts,  # type: ignore  # TODO: fix
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,
            enforce_collate=enforce_collate,
            ssl_enable=self.data.ssl_enable,
            ssl_ca=self.data.ssl_ca,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(
            source_type=SOURCE_TYPE_PG_SUBSELECT,
            field_doc_key="PG_SUBSELECT/subsql",
            localizer=localizer,
        )

    @property
    def allow_public_usage(self) -> bool:
        return True

from __future__ import annotations

from typing import ClassVar

import attr

from bi_core.base_models import ConnMDBMixin
from bi_core.i18n.localizer_base import Localizer
from bi_core.us_connection_base import ClassicConnectionSQL, DataSourceTemplate

from bi_connector_mysql.core.constants import SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT
from bi_connector_mysql.core.dto import MySQLConnDTO


class ConnectionMySQL(ClassicConnectionSQL):
    source_type = SOURCE_TYPE_MYSQL_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ConnMDBMixin, ClassicConnectionSQL.DataModel):
        pass

    def get_conn_dto(self) -> MySQLConnDTO:
        return MySQLConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),  # type: ignore  # TODO: fix
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(source_type=SOURCE_TYPE_MYSQL_SUBSELECT, localizer=localizer)

    @property
    def allow_public_usage(self) -> bool:
        return True

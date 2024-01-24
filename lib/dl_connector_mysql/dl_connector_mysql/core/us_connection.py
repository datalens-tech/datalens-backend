from __future__ import annotations

from typing import ClassVar

import attr

from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    DataSourceTemplate,
)
from dl_i18n.localizer_base import Localizer

from dl_connector_mysql.core.constants import (
    SOURCE_TYPE_MYSQL_SUBSELECT,
    SOURCE_TYPE_MYSQL_TABLE,
)
from dl_connector_mysql.core.dto import MySQLConnDTO


class ConnectionMySQL(ClassicConnectionSQL):
    source_type = SOURCE_TYPE_MYSQL_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        pass

    def get_conn_dto(self) -> MySQLConnDTO:
        return MySQLConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=self.parse_multihosts(),  # type: ignore  # TODO: fix
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,  # type: ignore  # 2024-01-24 # TODO: Argument "password" to "MySQLConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(source_type=SOURCE_TYPE_MYSQL_SUBSELECT, localizer=localizer)

    @property
    def allow_public_usage(self) -> bool:
        return True

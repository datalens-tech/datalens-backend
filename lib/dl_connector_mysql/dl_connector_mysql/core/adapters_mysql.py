from __future__ import annotations

from typing import ClassVar

import attr

from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connectors.base.error_transformer import DbErrorTransformer

from dl_connector_mysql.core.adapters_base_mysql import BaseMySQLAdapter
from dl_connector_mysql.core.error_transformer import sync_mysql_db_error_transformer
from dl_connector_mysql.core.target_dto import MySQLConnTargetDTO


@attr.s()
class MySQLAdapter(BaseMySQLAdapter, BaseClassicAdapter[MySQLConnTargetDTO]):
    execution_options = {
        "stream_results": True,
    }
    _error_transformer: ClassVar[DbErrorTransformer] = sync_mysql_db_error_transformer

    def get_connect_args(self) -> dict:
        return dict(charset="utf8", local_infile=0, ssl={"ssl_check_hostname": False})

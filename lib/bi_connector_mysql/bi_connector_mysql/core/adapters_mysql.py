from __future__ import annotations

from typing import ClassVar

import attr
from bi_core.connectors.base.error_transformer import DbErrorTransformer

from bi_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from bi_connector_mysql.core.adapters_base_mysql import BaseMySQLAdapter
from bi_connector_mysql.core.error_transformer import sync_mysql_db_error_transformer
from bi_connector_mysql.core.target_dto import MySQLConnTargetDTO


@attr.s()
class MySQLAdapter(BaseMySQLAdapter, BaseClassicAdapter[MySQLConnTargetDTO]):
    execution_options = {
        'stream_results': True,
    }
    _error_transformer: ClassVar[DbErrorTransformer] = sync_mysql_db_error_transformer

    def get_connect_args(self) -> dict:
        return dict(
            charset='utf8',
            local_infile=0,
        )

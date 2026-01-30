from __future__ import annotations

import attr

from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter

from dl_connector_starrocks.core.adapters_base_starrocks import BaseStarRocksAdapter
from dl_connector_starrocks.core.error_transformer import sync_starrocks_db_error_transformer
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


@attr.s()
class StarRocksAdapter(BaseStarRocksAdapter, BaseClassicAdapter[StarRocksConnTargetDTO]):
    _target_dto: StarRocksConnTargetDTO = attr.ib()

    execution_options = {
        "stream_results": True,
    }

    _error_transformer = sync_starrocks_db_error_transformer

    def get_connect_args(self) -> dict:
        return dict(
            super().get_connect_args(),
            charset="utf8",
            local_infile=0,
        )

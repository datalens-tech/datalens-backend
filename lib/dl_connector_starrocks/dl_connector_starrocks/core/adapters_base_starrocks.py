import attr

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


@attr.s(cmp=False)
class BaseStarRocksAdapter:
    _target_dto: StarRocksConnTargetDTO = attr.ib()

    conn_type = CONNECTION_TYPE_STARROCKS

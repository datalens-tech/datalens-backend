from __future__ import annotations

import attr

from dl_connector_ydb.core.ydb.constants import YDBAuthTypeMode
from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO


@attr.s(frozen=True)
class YDBConnTargetDTO(BaseSQLConnTargetDTO):
    auth_type: YDBAuthTypeMode = attr.ib()

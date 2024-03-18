from __future__ import annotations

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from dl_core.utils import secrepr


@attr.s(frozen=True)
class YDBConnTargetDTO(BaseSQLConnTargetDTO):
    token: str = attr.ib(repr=secrepr)
    auth_type: str = attr.ib()

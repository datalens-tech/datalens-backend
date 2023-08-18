from __future__ import annotations

import attr

from bi_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO


@attr.s
class PromQLConnTargetDTO(BaseSQLConnTargetDTO):
    protocol: str = attr.ib()

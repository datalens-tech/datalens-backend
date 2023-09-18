from __future__ import annotations

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO


@attr.s(frozen=True)
class YQConnTargetDTO(BaseSQLConnTargetDTO):
    """..."""

    cloud_id: str = attr.ib()
    folder_id: str = attr.ib()

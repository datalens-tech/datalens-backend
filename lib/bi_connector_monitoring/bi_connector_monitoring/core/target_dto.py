from __future__ import annotations

from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from dl_core.utils import secrepr


@attr.s
class MonitoringConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    url_path: str = attr.ib()
    iam_token: Optional[str] = attr.ib(default=None, repr=secrepr)
    folder_id: Optional[str] = attr.ib(default=None)

    def get_effective_host(self) -> Optional[str]:
        return self.host

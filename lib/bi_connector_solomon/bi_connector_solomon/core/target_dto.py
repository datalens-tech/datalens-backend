from __future__ import annotations

from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO


@attr.s
class SolomonConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    cookie_session_id: Optional[str] = attr.ib(default=None, repr=False)
    cookie_sessionid2: Optional[str] = attr.ib(default=None, repr=False)
    user_ip: Optional[str] = attr.ib(default=None)
    user_host: Optional[str] = attr.ib(default=None)

    def get_effective_host(self) -> Optional[str]:
        return self.host

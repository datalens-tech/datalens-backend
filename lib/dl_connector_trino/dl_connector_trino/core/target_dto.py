from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO

from dl_connector_trino.core.constants import TrinoAuthType


@attr.s(frozen=True)
class TrinoConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    auth_type: TrinoAuthType = attr.ib(kw_only=True, default=TrinoAuthType.NONE)
    password: Optional[str] = attr.ib(repr=False, kw_only=True, default=None)
    jwt: Optional[str] = attr.ib(repr=False, kw_only=True, default=None)
    ssl_ca: Optional[str] = attr.ib(repr=False, kw_only=True, default=None)

    def get_effective_host(self) -> Optional[str]:
        return self.host

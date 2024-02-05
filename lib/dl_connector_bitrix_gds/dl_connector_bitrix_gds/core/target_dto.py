from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseAiohttpConnTargetDTO
from dl_core.utils import secrepr


def hide_pass(value: Optional[dict]) -> str:
    if value is None:
        return repr(value)
    if not value:
        return repr(value)
    return repr({k: v for k, v in value.items() if k != "password"})


@attr.s(frozen=True)
class BitrixGDSConnTargetDTO(BaseAiohttpConnTargetDTO):
    portal: str = attr.ib(kw_only=True)
    token: str = attr.ib(kw_only=True, repr=secrepr)

    max_execution_time: Optional[int] = attr.ib()
    connect_timeout: Optional[int] = attr.ib()
    total_timeout: Optional[int] = attr.ib()

    redis_conn_params: Optional[dict] = attr.ib(repr=hide_pass)
    redis_caches_ttl: Optional[int] = attr.ib()

    def get_effective_host(self) -> Optional[str]:
        return None  # Not Applicable

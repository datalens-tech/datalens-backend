import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseAiohttpConnTargetDTO
from dl_core.utils import secrepr


def hide_pass(value: dict | None) -> str:
    if value is None:
        return repr(value)
    if not value:
        return repr(value)
    return repr({k: v for k, v in value.items() if k != "password"})


@attr.s(frozen=True)
class BitrixGDSConnTargetDTO(BaseAiohttpConnTargetDTO):
    portal: str = attr.ib(kw_only=True)
    token: str = attr.ib(kw_only=True, repr=secrepr)

    max_execution_time: int | None = attr.ib()
    connect_timeout: int | None = attr.ib()
    total_timeout: int | None = attr.ib()

    redis_conn_params: dict | None = attr.ib(repr=hide_pass)
    redis_caches_ttl: int | None = attr.ib()

    def get_effective_host(self) -> str | None:
        return None  # Not Applicable

import attr

from typing import Optional

from bi_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO


@attr.s(frozen=True)
class GSheetsConnTargetDTO(ConnTargetDTO):
    sheets_url: str = attr.ib(repr=False)

    max_execution_time: Optional[int] = attr.ib()
    connect_timeout: Optional[int] = attr.ib()
    total_timeout: Optional[int] = attr.ib()
    use_gozora: bool = attr.ib(kw_only=True, default=False)

    def get_effective_host(self) -> Optional[str]:
        return None  # Not Applicable

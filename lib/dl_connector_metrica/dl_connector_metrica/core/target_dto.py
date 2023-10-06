from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from dl_core.utils import secrepr


@attr.s(frozen=True)
class BaseMetricaAPIConnTargetDTO(ConnTargetDTO):
    token: str = attr.ib(repr=secrepr)
    accuracy: float = attr.ib()

    def get_effective_host(self) -> Optional[str]:
        return None


@attr.s(frozen=True)
class MetricaAPIConnTargetDTO(BaseMetricaAPIConnTargetDTO):
    pass


@attr.s(frozen=True)
class AppMetricaAPIConnTargetDTO(BaseMetricaAPIConnTargetDTO):
    pass

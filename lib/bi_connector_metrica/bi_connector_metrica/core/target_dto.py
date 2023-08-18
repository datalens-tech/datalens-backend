from typing import Optional

import attr

from bi_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from bi_core.utils import secrepr


@attr.s(frozen=True)
class BaseMetricaAPIConnTargetDTO(ConnTargetDTO):
    token: str = attr.ib(repr=secrepr)
    accuracy: float = attr.ib()

    def get_effective_host(self) -> Optional[str]:
        return None


class MetricaAPIConnTargetDTO(BaseMetricaAPIConnTargetDTO):
    pass


@attr.s(frozen=True)
class AppMetricaAPIConnTargetDTO(BaseMetricaAPIConnTargetDTO):
    pass

from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from dl_core.utils import secrepr


@attr.s(frozen=True, kw_only=True)
class BigQueryConnTargetDTO(ConnTargetDTO):
    credentials: str = attr.ib(repr=secrepr)  # corresponds to `credentials_base64` in BQ SQLA
    project_id: str = attr.ib()

    def get_effective_host(self) -> Optional[str]:
        return None

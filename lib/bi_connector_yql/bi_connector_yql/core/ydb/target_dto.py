from __future__ import annotations

from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from dl_core.utils import secrepr


@attr.s(frozen=True)
class YDBConnTargetDTO(BaseSQLConnTargetDTO):
    # Passed from `YDBConnectOptions`
    is_cloud: bool = attr.ib(default=False)
    iam_token: Optional[str] = attr.ib(default=None, repr=secrepr)

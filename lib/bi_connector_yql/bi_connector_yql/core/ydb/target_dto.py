from __future__ import annotations

from typing import Optional

import attr

from bi_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from bi_core.utils import secrepr


@attr.s(frozen=True)
class YDBConnTargetDTO(BaseSQLConnTargetDTO):
    # Passed from `YDBConnectOptions`
    # (which is mutated in `CloudEnvManagerFactory`)
    is_cloud: bool = attr.ib(default=False)
    iam_token: Optional[str] = attr.ib(default=None, repr=secrepr)

from typing import Optional

import attr

from dl_constants.types import TJSONLike
from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO


@attr.s(frozen=True)
class MySQLConnTargetDTO(BaseSQLConnTargetDTO):
    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        return {
            **super().to_jsonable_dict(),
            "ssl_enable": self.ssl_enable,
            "ssl_ca": self.ssl_ca,
        }

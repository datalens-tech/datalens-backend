from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr
import typing_extensions

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO

from dl_connector_ydb.core.ydb.constants import YDBAuthTypeMode


if TYPE_CHECKING:
    from dl_constants.types import TJSONLike


@attr.s(frozen=True)
class YDBConnTargetDTO(BaseSQLConnTargetDTO):
    auth_type: YDBAuthTypeMode = attr.ib()

    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        return {
            **super().to_jsonable_dict(),
            "auth_type": self.auth_type.name,
        }

    @classmethod
    def _from_jsonable_dict(cls, data: dict) -> typing_extensions.Self:
        return cls(
            **{
                **data,
                "auth_type": YDBAuthTypeMode[data["auth_type"]],
            }
        )

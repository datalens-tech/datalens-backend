from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Type,
    TypeVar,
)

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO

from dl_connector_ydb.core.ydb.constants import YDBAuthTypeMode


if TYPE_CHECKING:
    from dl_constants.types import TJSONLike

_CT_DTO_TV = TypeVar("_CT_DTO_TV", bound="YDBConnTargetDTO")


@attr.s(frozen=True)
class YDBConnTargetDTO(BaseSQLConnTargetDTO):
    auth_type: YDBAuthTypeMode = attr.ib()

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        return {
            **super().to_jsonable_dict(),
            "auth_type": self.auth_type.name,
        }

    @classmethod
    def _from_jsonable_dict(cls: Type[_CT_DTO_TV], data: dict) -> _CT_DTO_TV:
        return cls(
            **{  # type: ignore  # TODO: fix
                **data,
                "auth_type": YDBAuthTypeMode[data["auth_type"]],
            }
        )

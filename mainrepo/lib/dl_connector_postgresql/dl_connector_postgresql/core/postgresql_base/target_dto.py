from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
    Type,
    TypeVar,
)

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO

from dl_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode

if TYPE_CHECKING:
    from dl_constants.types import TJSONLike


_CT_DTO_TV = TypeVar("_CT_DTO_TV", bound="PostgresConnTargetDTO")


@attr.s(frozen=True)
class PostgresConnTargetDTO(BaseSQLConnTargetDTO):
    enforce_collate: PGEnforceCollateMode = attr.ib(kw_only=True, default=PGEnforceCollateMode.off)
    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        return {
            **super().to_jsonable_dict(),
            "enforce_collate": self.enforce_collate.name,
            "ssl_enable": self.ssl_enable,
            "ssl_ca": self.ssl_ca,
        }

    @classmethod
    def _from_jsonable_dict(cls: Type[_CT_DTO_TV], data: dict) -> _CT_DTO_TV:
        return cls(
            **{  # type: ignore  # TODO: fix
                **data,
                "enforce_collate": PGEnforceCollateMode[data["enforce_collate"]],
            }
        )

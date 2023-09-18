from typing import (
    Optional,
    Type,
    TypeVar,
)

import attr

from dl_connector_chyt.core.target_dto import BaseCHYTConnTargetDTO

_CT_DTO_TV = TypeVar("_CT_DTO_TV", bound="BaseCHYTInternalConnTargetDTO")


@attr.s(frozen=True)
class BaseCHYTInternalConnTargetDTO(BaseCHYTConnTargetDTO):
    yt_cluster: str = attr.ib()

    @classmethod
    def _from_jsonable_dict(cls: Type[_CT_DTO_TV], data: dict) -> _CT_DTO_TV:
        prepared_data = {
            **data,
            "mirroring_conn_target_dto": cls(**data["mirroring_conn_target_dto"])
            if data["mirroring_conn_target_dto"] is not None
            else None,
        }
        return cls(**prepared_data)  # type: ignore


@attr.s(frozen=True)
class CHYTInternalConnTargetDTO(BaseCHYTInternalConnTargetDTO):
    mirroring_conn_target_dto: Optional["CHYTInternalConnTargetDTO"] = attr.ib()


@attr.s(frozen=True)
class CHYTUserAuthConnTargetDTO(BaseCHYTInternalConnTargetDTO):
    mirroring_conn_target_dto: Optional["CHYTUserAuthConnTargetDTO"] = attr.ib()

    header_authorization: Optional[str] = attr.ib(repr=False)
    header_cookie: Optional[str] = attr.ib(repr=False)
    header_csrf_token: Optional[str] = attr.ib(repr=False)

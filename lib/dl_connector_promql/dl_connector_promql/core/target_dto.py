import attr
from typing_extensions import Self

from dl_constants.types import TJSONLike
from dl_core.connection_executors.models.connection_target_dto_base import (
    BaseAiohttpConnTargetDTO,
    BaseSQLConnTargetDTO,
)

from dl_connector_promql.core.constants import PromQLAuthType


@attr.s
class PromQLConnTargetDTO(BaseSQLConnTargetDTO, BaseAiohttpConnTargetDTO):
    path: str = attr.ib()
    protocol: str = attr.ib()
    auth_type: PromQLAuthType = attr.ib()
    auth_header: str | None = attr.ib(repr=False)

    @classmethod
    def _from_jsonable_dict(cls, data: dict) -> Self:
        if "auth_type" in data:
            data["auth_type"] = PromQLAuthType(data["auth_type"])
        return cls(**data)

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        data = super().to_jsonable_dict()
        data["auth_type"] = self.auth_type.value
        return data

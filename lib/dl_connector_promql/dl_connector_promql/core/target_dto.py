import attr
from typing_extensions import Self

from dl_constants.types import TJSONLike
from dl_core.connection_executors.models.connection_target_dto_base import (
    BaseAiohttpConnTargetDTO,
    BaseSQLConnTargetDTO,
)
from dl_core.utils import secrepr

from dl_connector_promql.core.constants import PromQLAuthType


@attr.s
class PromQLConnTargetDTO(BaseSQLConnTargetDTO, BaseAiohttpConnTargetDTO):
    # PromQL supports auth_type=token/other where credentials come via auth_header,
    # so username/password may legitimately be None here. This narrows the parent
    # contract (BaseSQLConnTargetDTO declares them as str), hence the ignores.
    username: str | None = attr.ib()  # type: ignore[assignment]
    password: str | None = attr.ib(repr=secrepr)  # type: ignore[assignment]
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

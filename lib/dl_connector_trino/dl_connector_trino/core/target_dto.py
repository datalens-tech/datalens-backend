from typing import Optional

import attr
from typing_extensions import Self

from dl_constants.types import TJSONLike
from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO

from dl_connector_trino.core.constants import TrinoAuthType


@attr.s(frozen=True)
class TrinoConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    auth_type: TrinoAuthType = attr.ib(kw_only=True)
    password: Optional[str] = attr.ib(repr=False, kw_only=True, default=None)
    jwt: Optional[str] = attr.ib(repr=False, kw_only=True, default=None)
    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    def get_effective_host(self) -> Optional[str]:
        return self.host

    @classmethod
    def _from_jsonable_dict(cls, data: dict) -> Self:
        data["auth_type"] = TrinoAuthType(data["auth_type"])
        return cls(**data)

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        jsonable_dict = super().to_jsonable_dict()
        jsonable_dict["auth_type"] = self.auth_type.value
        return jsonable_dict

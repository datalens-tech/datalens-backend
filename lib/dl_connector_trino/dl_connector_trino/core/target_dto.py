import attr
from typing_extensions import Self

from dl_constants.types import TJSONLike
from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO

from dl_connector_trino.core.constants import TrinoAuthType


@attr.s(frozen=True, kw_only=True)
class TrinoConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    auth_type: TrinoAuthType = attr.ib()
    password: str | None = attr.ib(repr=False, default=None)
    jwt: str | None = attr.ib(repr=False, default=None)
    ssl_enable: bool = attr.ib(default=False)
    ssl_ca: str | None = attr.ib(default=None)
    connect_timeout: int | None = attr.ib(default=None)
    max_execution_time: int | None = attr.ib(default=None)
    total_timeout: int | None = attr.ib(default=None)

    def get_effective_host(self) -> str | None:
        return self.host

    @classmethod
    def _from_jsonable_dict(cls, data: dict) -> Self:
        data["auth_type"] = TrinoAuthType(data["auth_type"])
        return cls(**data)

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        jsonable_dict = super().to_jsonable_dict()
        jsonable_dict["auth_type"] = self.auth_type.value
        return jsonable_dict

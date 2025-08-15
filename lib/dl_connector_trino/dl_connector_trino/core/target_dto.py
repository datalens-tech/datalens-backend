import logging

import attr
from typing_extensions import Self

from dl_constants.types import TJSONLike
from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO

from dl_connector_trino.core.constants import TrinoAuthType


LOGGER = logging.getLogger(__name__)


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

    def __attrs_post_init__(self) -> None:
        LOGGER.debug(
            "TrinoConnTargetDTO initialized with host=%s, port=%s, username=%s, auth_type=%s, password=%s, jwt=%s, ssl_enable=%s, ssl_ca=%s",
            self.host,
            self.port,
            self.username,
            self.auth_type,
            self.password,
            self.jwt,
            self.ssl_enable,
            self.ssl_ca,
            stack_info=True,
        )

        try:
            raise Exception("TrinoConnTargetDTO initialized")
        except Exception as e:
            LOGGER.exception(
                "TrinoConnTargetDTO initialization exception: %s",
                e,
                stack_info=True,
            )


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

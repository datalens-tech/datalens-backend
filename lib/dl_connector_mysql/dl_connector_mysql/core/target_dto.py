import attr

from dl_constants.types import TJSONLike
from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO

from dl_connector_mysql.core.constants import MySQLEnforceCollateMode


@attr.s(frozen=True)
class MySQLConnTargetDTO(BaseSQLConnTargetDTO):
    enforce_collate: MySQLEnforceCollateMode = attr.ib(kw_only=True, default=MySQLEnforceCollateMode.off)
    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: str | None = attr.ib(kw_only=True, default=None)

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        return {
            **super().to_jsonable_dict(),
            "enforce_collate": self.enforce_collate.name,
            "ssl_enable": self.ssl_enable,
            "ssl_ca": self.ssl_ca,
        }

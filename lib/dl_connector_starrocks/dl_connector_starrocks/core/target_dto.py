import attr

from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO


@attr.s(frozen=True, kw_only=True)
class StarRocksConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    password: str = attr.ib(repr=False)
    ssl_enable: bool = attr.ib(default=False)
    ssl_ca: str | None = attr.ib(default=None)

    def get_effective_host(self) -> str | None:
        return self.host

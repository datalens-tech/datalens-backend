import attr

from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO


@attr.s(frozen=True, kw_only=True)
class StarRocksConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    password: str = attr.ib(repr=False)

    def get_effective_host(self) -> str | None:
        return self.host

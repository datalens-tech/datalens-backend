import attr

from dl_core.connection_models.conn_options import ConnectOptions


@attr.s(frozen=True, hash=True)
class TrinoConnectOptions(ConnectOptions):
    connect_timeout: int | None = attr.ib(default=None)
    max_execution_time: int | None = attr.ib(default=None)
    total_timeout: int | None = attr.ib(default=None)

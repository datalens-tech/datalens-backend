from typing import Sequence

import attr


@attr.s
class PGConfig:
    host_list: Sequence[str] = attr.ib()
    port: int = attr.ib()
    database: str = attr.ib()
    user: str = attr.ib()
    password: str = attr.ib(repr=False)
    ssl_mode: str = attr.ib()
    connect_timeout: float = attr.ib()

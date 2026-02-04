from __future__ import annotations

from typing import (
    Callable,
)

from dl_core.connection_models import ConnectOptions
from dl_core.us_connection_base import ConnectionBase


ConnectOptionsFactory = Callable[[ConnectionBase], ConnectOptions | None]
ConnectOptionsMutator = Callable[[ConnectOptions], ConnectOptions]

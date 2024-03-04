from __future__ import annotations

from typing import (
    Callable,
    Optional,
)

from dl_core.connection_models import ConnectOptions
from dl_core.us_connection_base import ConnectionBase


ConnectOptionsFactory = Callable[[ConnectionBase], Optional[ConnectOptions]]
ConnectOptionsMutator = Callable[[ConnectOptions], ConnectOptions]

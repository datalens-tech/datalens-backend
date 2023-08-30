from __future__ import annotations

from typing import Callable, Optional

from bi_core.us_connection_base import ExecutorBasedMixin
from bi_core.connection_models import ConnectOptions

ConnectOptionsFactory = Callable[[ExecutorBasedMixin], Optional[ConnectOptions]]
ConnectOptionsMutator = Callable[[ConnectOptions], ConnectOptions]

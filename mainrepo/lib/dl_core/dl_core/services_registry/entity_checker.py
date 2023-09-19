from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)


if TYPE_CHECKING:
    from dl_api_commons.base_models import RequestContextInfo
    from dl_core.us_connection_base import ConnectionBase
    from dl_core.us_dataset import Dataset
    from dl_core.us_manager.us_manager import USManagerBase


class EntityUsageNotAllowed(Exception):
    _user_reason: Optional[str]

    def __init__(self, *args: Any, user_reason: Optional[str] = None):
        super().__init__(*args)
        self._user_reason = user_reason

    @property
    def user_reason(self) -> str:
        return self._user_reason if self._user_reason else "Unknown reason"


class EntityUsageChecker(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def ensure_dataset_can_be_used(
        self,
        rci: RequestContextInfo,
        dataset: Dataset,
        us_manager: USManagerBase,
    ) -> None:
        pass

    @abc.abstractmethod
    def ensure_data_connection_can_be_used(self, rci: RequestContextInfo, conn: ConnectionBase):  # type: ignore  # TODO: fix
        pass

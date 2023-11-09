from typing import Optional

from dl_constants.enums import NotificationLevel
from dl_core.reporting.notifications import BaseNotification

from dl_connector_bundle_chs3.chs3_base.core.constants import (
    NOTIF_TYPE_DATA_UPDATE_FAILURE,
    NOTIF_TYPE_STALE_DATA,
)


class StaleDataNotification(BaseNotification):
    type = NOTIF_TYPE_STALE_DATA
    _title = "Stale data"
    _message = "The data has not been updated for more than 30 minutes, a background update is in progress"
    _level = NotificationLevel.info


class DataUpdateFailureNotification(BaseNotification):
    def __init__(self, err_code: str, request_id: Optional[str]) -> None:
        super().__init__()
        self.err_code = err_code
        self.request_id = request_id or "unknown"

    type = NOTIF_TYPE_DATA_UPDATE_FAILURE
    _title = "Data update failed"
    _message = (
        "The displayed data may be outdated due to the failure of the last update.\n"
        "Reason: {err_code}, Request-ID: {request_id}."
    )
    _level = NotificationLevel.warning

    @property
    def message(self) -> str:
        return self._message.format(err_code=self.err_code, request_id=self.request_id)

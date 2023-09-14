import time
from typing import (
    Any,
    Callable,
    Optional,
    Type,
)

from bi_api_commons.reporting.models import NotificationReportingRecord
from bi_constants.enums import (
    NotificationLevel,
    NotificationType,
)


class BaseNotification:
    type: NotificationType

    _title: str
    _message: str
    _level: NotificationLevel

    def __init__(self, **kwargs: Any) -> None:
        pass

    @property
    def title(self) -> str:
        return self._title

    @property
    def message(self) -> str:
        return self._message

    @property
    def level(self) -> NotificationLevel:
        return self._level

    @property
    def locator(self) -> str:
        return self.type.value


_NOTIFICATIONS: dict[NotificationType, Type[BaseNotification]] = {}


# TODO: add control (via config/env) over which notifications should not be displayed
def register_notification() -> Callable[[Type[BaseNotification]], Type[BaseNotification]]:
    def wrap(cls: Type[BaseNotification]) -> Type[BaseNotification]:
        _NOTIFICATIONS[cls.type] = cls
        return cls

    return wrap


@register_notification()
class TotalsRemovedDueToMeasureFilterNotification(BaseNotification):
    type = NotificationType.totals_removed_due_to_measure_filter
    _title = "No totals shown"
    _message = "Totals have been removed because there is filtering by measure"
    _level = NotificationLevel.info


def get_notification_record(
    notification_type: NotificationType, **kwargs: Any
) -> Optional[NotificationReportingRecord]:
    ntf_cls = _NOTIFICATIONS.get(notification_type)
    if ntf_cls is None:
        return None
    notification = ntf_cls(**kwargs)
    return NotificationReportingRecord(
        title=notification.title,
        message=notification.message,
        level=notification.level,
        locator=notification.locator,
        timestamp=time.time(),
    )

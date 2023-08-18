import itertools
import time
from typing import Any, Callable, Optional, Sequence, Type

from dynamic_enum import DynamicEnum, AutoEnumValue

from bi_configs.enums import AppType
from bi_configs.env_var_reader import get_from_env
from bi_configs.utils import app_type_env_var_converter
from bi_constants.enums import NotificationLevel

from bi_api_commons.reporting.models import NotificationReportingRecord


class NotificationType(DynamicEnum):
    totals_removed_due_to_measure_filter = AutoEnumValue()
    using_public_clickhouse_clique = AutoEnumValue()
    stale_data = AutoEnumValue()
    data_update_failure = AutoEnumValue()


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

INTRANET_APP_TYPES = [AppType.INTRANET, AppType.TESTS]
CLOUD_APP_TYPES = [AppType.CLOUD, AppType.CLOUD_PUBLIC]
OTHER_APP_TYPES = [app_type for app_type in list(AppType) if app_type not in itertools.chain(INTRANET_APP_TYPES, CLOUD_APP_TYPES)]


# TODO: better way to customize notification based on environment?
def register_notification(app_types: Optional[Sequence[AppType]] = None) -> Callable[[Type[BaseNotification]], Type[BaseNotification]]:
    def wrap(cls: Type[BaseNotification]) -> Type[BaseNotification]:
        if app_types is None or get_from_env('YENV_TYPE', app_type_env_var_converter, None) in app_types:
            _NOTIFICATIONS[cls.type] = cls
        return cls
    return wrap


@register_notification()
class TotalsRemovedDueToMeasureFilterNotification(BaseNotification):
    type = NotificationType.totals_removed_due_to_measure_filter
    _title = 'No totals shown'
    _message = 'Totals have been removed because there is filtering by measure'
    _level = NotificationLevel.info


@register_notification()
class UsingPublicClickhouseCliqueNotification(BaseNotification):
    type = NotificationType.using_public_clickhouse_clique
    _title = 'Using public clique'
    _message = 'Public clique has no guarantees on availability and resources, avoid using it in important processes'
    _level = NotificationLevel.info


@register_notification()
class StaleDataNotification(BaseNotification):
    type = NotificationType.stale_data
    _title = 'Stale data'
    _message = 'The data has not been updated for more than 30 minutes, a background update is in progress'
    _level = NotificationLevel.info


@register_notification()
class DataUpdateFailureNotification(BaseNotification):
    def __init__(self, err_code: str, request_id: Optional[str]) -> None:
        super().__init__()
        self.err_code = err_code
        self.request_id = request_id or 'unknown'

    type = NotificationType.data_update_failure
    _title = 'Data update failed'
    _message = (
        'The displayed data may be outdated due to the failure of the last update.\n'
        'Reason: {err_code}, Request-ID: {request_id}.'
    )
    _level = NotificationLevel.warning

    @property
    def message(self) -> str:
        return self._message.format(err_code=self.err_code, request_id=self.request_id)


def get_notification_record(
    notification_type: NotificationType,
    **kwargs: Any
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

from dl_constants.enums import NotificationLevel

from dl_core.reporting.notifications import BaseNotification

from bi_connector_chyt_internal.core.constants import NOTIF_TYPE_CHYT_USING_PUBLIC_CLIQUE


class UsingPublicClickhouseCliqueNotification(BaseNotification):
    type = NOTIF_TYPE_CHYT_USING_PUBLIC_CLIQUE
    _title = 'Using public clique'
    _message = 'Public clique has no guarantees on availability and resources, avoid using it in important processes'
    _level = NotificationLevel.info

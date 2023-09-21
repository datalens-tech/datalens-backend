from __future__ import annotations

from dl_api_connector.connection_info import ConnectionInfoProvider
from dl_connector_chyt.api.i18n.localizer import Translatable


class CHYTConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-chyt")

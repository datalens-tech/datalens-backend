from __future__ import annotations

from bi_api_connector.connection_info import ConnectionInfoProvider
from bi_connector_solomon.bi.i18n.localizer import Translatable


class SolomonConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-solomon')

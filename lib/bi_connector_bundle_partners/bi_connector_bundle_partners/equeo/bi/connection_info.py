from __future__ import annotations

from bi_api_connector.connection_info import ConnectionInfoProvider
from bi_connector_bundle_partners.base.bi.i18n.localizer import Translatable


class EqueoConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-equeo')

from __future__ import annotations

from bi_api_connector.connection_info import ConnectionInfoProvider
from bi_api_lib.i18n.localizer import Translatable


class BitrixGDSConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-bitrix')

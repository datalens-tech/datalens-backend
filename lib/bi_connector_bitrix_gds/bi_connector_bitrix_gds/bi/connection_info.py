from __future__ import annotations

from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_bitrix_gds.bi.i18n.localizer import Translatable


class BitrixGDSConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-bitrix")

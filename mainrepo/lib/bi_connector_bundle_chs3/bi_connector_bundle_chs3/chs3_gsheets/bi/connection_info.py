from __future__ import annotations

from bi_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_bundle_chs3.chs3_base.bi.i18n.localizer import Translatable


class GSheetsFileS3ConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-gsheets_v2")

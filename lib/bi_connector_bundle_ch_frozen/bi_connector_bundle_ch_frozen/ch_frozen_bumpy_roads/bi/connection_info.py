from __future__ import annotations

from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.i18n.localizer import Translatable


class CHFrozenBumpyRoadsConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-ch_frozen_bumpy_roads")

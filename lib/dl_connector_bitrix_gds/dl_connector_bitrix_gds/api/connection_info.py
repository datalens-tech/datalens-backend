from __future__ import annotations

from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_bitrix_gds.api.i18n.localizer import Translatable
from dl_connector_bitrix_gds.assets.icons import (
    nav,
    standard,
)


class BitrixGDSConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-bitrix")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("bitrix24.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("bitrix24.svg")
        super().__attrs_post_init__()

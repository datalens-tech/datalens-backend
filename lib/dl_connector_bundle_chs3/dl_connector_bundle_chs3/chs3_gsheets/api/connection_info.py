from __future__ import annotations

from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_bundle_chs3.chs3_base.api.i18n.localizer import Translatable
from dl_connector_bundle_chs3.chs3_gsheets.assets.icons import (
    nav,
    standard,
)


class GSheetsFileS3ConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-gsheets_v2")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("gsheets.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("gsheets.svg")
        super().__attrs_post_init__()

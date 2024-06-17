from __future__ import annotations

from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_chyt.api.i18n.localizer import Translatable
from dl_connector_chyt.assets.icons import (
    nav,
    standard,
)


class CHYTConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-chyt")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("chyt.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("chyt.svg")
        super().__attrs_post_init__()

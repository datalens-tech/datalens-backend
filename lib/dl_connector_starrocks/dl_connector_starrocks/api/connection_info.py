from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_starrocks.api.i18n.localizer import Translatable
from dl_connector_starrocks.assets.icons import (
    nav,
    standard,
)


class StarRocksConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-starrocks")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("starrocks.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("starrocks.svg")
        super().__attrs_post_init__()

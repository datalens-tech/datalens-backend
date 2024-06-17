from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_ydb.api.ydb.i18n.localizer import Translatable
from dl_connector_ydb.assets.icons import (
    nav,
    standard,
)


class YDBConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-ydb")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("ydb.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("ydb.svg")
        super().__attrs_post_init__()

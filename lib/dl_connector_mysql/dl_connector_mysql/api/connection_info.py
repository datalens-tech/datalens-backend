from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_mysql.api.i18n.localizer import Translatable
from dl_connector_mysql.assets.icons import (
    nav,
    standard,
)


class MySQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-mysql")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("mysql.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("mysql.svg")
        super().__attrs_post_init__()

from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_mssql.api.i18n.localizer import Translatable
from dl_connector_mssql.assets.icons import (
    nav,
    standard,
)


class MSSQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-mssql")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("mssql.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("mssql.svg")
        super().__attrs_post_init__()

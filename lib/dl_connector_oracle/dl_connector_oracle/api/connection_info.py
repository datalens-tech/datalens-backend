from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_oracle.api.i18n.localizer import Translatable
from dl_connector_oracle.assets.icons import (
    nav,
    standard,
)


class OracleConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-oracle")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("oracle.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("oracle.svg")
        super().__attrs_post_init__()

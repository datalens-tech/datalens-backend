from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_trino.api.i18n.localizer import Translatable
from dl_connector_trino.assets.icons import (
    nav,
    standard,
)


class TrinoConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-trino")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("trino.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("trino.svg")
        super().__attrs_post_init__()

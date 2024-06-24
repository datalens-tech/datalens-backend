from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_promql.api.i18n.localizer import Translatable
from dl_connector_promql.assets.icons import (
    nav,
    standard,
)


class PromQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-promql")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("promql.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("promql.svg")
        super().__attrs_post_init__()

from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_clickhouse.api.i18n.localizer import Translatable
from dl_connector_clickhouse.assets.icons import (
    nav,
    standard,
)


class ClickHouseConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-clickhouse")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("clickhouse.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("clickhouse.svg")
        super().__attrs_post_init__()

from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_bigquery.api.i18n.localizer import Translatable
from dl_connector_bigquery.assets.icons import (
    nav,
    standard,
)


class BigQueryConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-bigquery")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("bigquery.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("bigquery.svg")
        super().__attrs_post_init__()

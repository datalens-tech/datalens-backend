from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_metrica.api.i18n.localizer import Translatable
from dl_connector_metrica.assets.icons import (
    nav,
    standard,
)


class MetricaConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-metrica")
    alias = "metrica"

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("metrika_api.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("metrika_api.svg")
        super().__attrs_post_init__()


class AppMetricaConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-appmetrica")
    alias = "appmetrica"

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("appmetrica_api.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("appmetrica_api.svg")
        super().__attrs_post_init__()

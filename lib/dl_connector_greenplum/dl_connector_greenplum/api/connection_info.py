from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_greenplum.api.i18n.localizer import Translatable
from dl_connector_greenplum.assets.icons import (
    nav,
    standard,
)


class GreenplumConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-greenplum")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("greenplum.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("greenplum.svg")
        super().__attrs_post_init__()

from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_snowflake.api.i18n.localizer import Translatable
from dl_connector_snowflake.assets.icons import (
    nav,
    standard,
)


class SnowflakeConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-snowflake")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("snowflake.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("snowflake.svg")
        super().__attrs_post_init__()

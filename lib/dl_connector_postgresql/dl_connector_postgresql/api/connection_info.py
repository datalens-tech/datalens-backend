from importlib.resources import files

from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_postgresql.api.i18n.localizer import Translatable
from dl_connector_postgresql.assets.icons import (
    nav,
    standard,
)


class PostgreSQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-postgres")

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard_filepath = files(standard).joinpath("postgres.svg")
        self.icon_data_nav_filepath = files(nav).joinpath("postgres.svg")
        super().__attrs_post_init__()

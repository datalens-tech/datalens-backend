import attr

from bi_core.connection_models.dto_defs import ConnDTO

from bi_connector_gsheets.core.constants import CONNECTION_TYPE_GSHEETS


@attr.s(frozen=True)
class GSheetsConnDTO(ConnDTO):
    conn_type = CONNECTION_TYPE_GSHEETS
    sheets_url: str = attr.ib(kw_only=True, repr=False)

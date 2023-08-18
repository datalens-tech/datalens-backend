import attr

from bi_constants.enums import ConnectionType

from bi_core.connection_models.dto_defs import ConnDTO


@attr.s(frozen=True)
class GSheetsConnDTO(ConnDTO):
    conn_type = ConnectionType.gsheets
    sheets_url: str = attr.ib(kw_only=True, repr=False)

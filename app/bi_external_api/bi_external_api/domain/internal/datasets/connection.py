import attr

from dl_constants.enums import ConnectionType

from ..dl_common import (
    EntryInstance,
    EntryScope,
    EntrySummary,
)


@attr.s(frozen=True, auto_attribs=True)
class BIConnectionSummary:
    common_summary: EntrySummary
    type: ConnectionType

    @property
    def id(self) -> str:
        return self.common_summary.id

    @property
    def name(self) -> str:
        return self.common_summary.name


@attr.s(frozen=True, auto_attribs=True)
class ConnectionInstance(EntryInstance):
    SCOPE = EntryScope.connection

    type: ConnectionType = attr.ib()

    @property
    def id(self) -> str:
        return self.summary.id

    @property
    def name(self) -> str:
        return self.summary.name

    def to_bi_connection_summary(self) -> BIConnectionSummary:
        return BIConnectionSummary(
            common_summary=self.summary,
            type=self.type,
        )

    @classmethod
    def create_from_bi_connection_summary(cls, conn_summary: BIConnectionSummary) -> "ConnectionInstance":
        return cls(
            summary=conn_summary.common_summary,
            type=conn_summary.type,
        )

    @classmethod
    def create_for_type(cls, conn_type: ConnectionType, *, id: str, name: str, wb_id: str) -> "ConnectionInstance":
        return cls(
            summary=EntrySummary(
                scope=EntryScope.connection,
                id=id,
                name=name,
                workbook_id=wb_id,
            ),
            type=conn_type,
        )

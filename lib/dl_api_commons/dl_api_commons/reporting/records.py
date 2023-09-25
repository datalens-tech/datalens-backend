from typing import (
    Any,
    Optional,
    TypeVar,
)

import attr


_RR_TV = TypeVar("_RR_TV", bound="ReportingRecord")


@attr.s(frozen=True, auto_attribs=True)
class ReportingRecord:
    timestamp: float

    def clone(self: _RR_TV, **kwargs: Any) -> _RR_TV:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True, auto_attribs=True)
class RequestResultReportingRecord(ReportingRecord):
    response_status_code: Optional[int] = 200
    err_code: Optional[str] = None

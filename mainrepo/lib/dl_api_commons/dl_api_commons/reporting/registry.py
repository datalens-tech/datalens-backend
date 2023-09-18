from __future__ import annotations

import abc
from typing import (
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.reporting.records import (
    ReportingRecord,
    RequestResultReportingRecord,
)

_RECORD_TV = TypeVar("_RECORD_TV", bound=ReportingRecord)


@attr.s
class ReportingRegistry(metaclass=abc.ABCMeta):
    _rci: RequestContextInfo = attr.ib()

    @property
    def rci(self) -> RequestContextInfo:
        return self._rci

    @rci.setter
    def rci(self, rci: RequestContextInfo) -> None:
        self._rci = rci

    @abc.abstractmethod
    def clear_records(self) -> None:
        pass

    @abc.abstractmethod
    def save_reporting_record(self, report: Optional[ReportingRecord]) -> None:
        pass

    @abc.abstractmethod
    def get_reporting_records(self) -> Tuple[ReportingRecord, ...]:
        pass

    @abc.abstractmethod
    def get_request_result_reporting_records(self) -> Tuple[RequestResultReportingRecord, ...]:
        pass

    @abc.abstractmethod
    def get_records_of_type(self, clz: Type[_RECORD_TV]) -> Tuple[_RECORD_TV, ...]:
        pass


@attr.s
class DefaultReportingRegistry(ReportingRegistry):
    _reporting_records: List[ReportingRecord] = attr.ib(init=False, factory=list)

    def clear_records(self) -> None:
        self._reporting_records.clear()

    def save_reporting_record(self, report: Optional[ReportingRecord]) -> None:
        if report is not None:
            self._reporting_records.append(report)

    def get_reporting_records(self) -> Tuple[ReportingRecord, ...]:
        return tuple(self._reporting_records)

    def get_request_result_reporting_records(self) -> Tuple[RequestResultReportingRecord, ...]:
        return tuple(r for r in self._reporting_records if isinstance(r, RequestResultReportingRecord))

    def get_records_of_type(self, clz: Type[_RECORD_TV]) -> Tuple[_RECORD_TV, ...]:
        return tuple(r for r in self._reporting_records if isinstance(r, clz))

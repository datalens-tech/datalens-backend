from collections.abc import Sequence

import attr

from bi_external_api.exc_defs import ExternalAPIException
from dl_api_commons.exc import ExceptionWithData


@attr.s()
class ConversionExcData:
    pass


class ConverterError(ExternalAPIException):
    pass


class WorkbookEntryNotFound(ConverterError):
    pass


class WorkbookEntryBroken(ConverterError):
    pass


class NotSupportedYet(ConverterError):
    pass


class MalformedEntryConfig(ConverterError):
    pass


class ConstraintViolationError(ConverterError):
    pass


class LimitExhausted(ConverterError):
    pass


class MissingGuidFormula(ConverterError):
    pass


class DatasetFieldNotFound(ConverterError):
    pass


@attr.s(frozen=True)
class UnsupportedInternalEntryData:
    kind: str = attr.ib()


# TODO FIX: Add add base converter error class with data
class UnsupportedInternalEntryError(ExternalAPIException, ExceptionWithData[UnsupportedInternalEntryData]):
    pass


@attr.s()
class CompositeConverterErrorData:
    map_path_exc: dict[Sequence[str], Exception] = attr.ib()


class CompositeConverterError(ExternalAPIException, ExceptionWithData[CompositeConverterErrorData]):
    pass

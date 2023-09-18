from typing import Optional

import attr

from dl_api_commons.exc import NotFoundErr, ExceptionWithData
from bi_external_api.domain.internal import datasets
from bi_external_api.exc_defs import ExternalAPIException


@attr.s()
class DatasetValidationErrorData:
    message: str = attr.ib()
    dataset: Optional[datasets.Dataset] = attr.ib()


#
# Common exceptions
#


class WorkbookNotFound(NotFoundErr):
    CODE = "WORKBOOK_NOT_FOUND"


class InvalidIDFormatError(Exception):
    pass


#
# Dataset API specific
#
class DatasetValidationError(ExternalAPIException, ExceptionWithData[DatasetValidationErrorData]):
    pass

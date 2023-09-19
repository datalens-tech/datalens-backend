from typing import TypeVar

from bi_external_api.domain import external as ext
from bi_external_api.exc_defs import ExternalAPIException
from dl_api_commons.exc import ExceptionWithData


_PUB_EXC_DATA_TV = TypeVar("_PUB_EXC_DATA_TV")


class PublicException(ExternalAPIException, ExceptionWithData[_PUB_EXC_DATA_TV]):
    pass


class WorkbookOperationException(PublicException[ext.ErrWorkbookOp]):
    pass

from __future__ import annotations

from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)
import dl_core.exc
import dl_formula.core.exc
import dl_query_processing.exc


def exc_cls_to_code(exc_cls):
    exc_cls_code = (
        getattr(exc_cls, "err_code", None)
        # formula excs.
        # TODO: document how it is built or use a common function.
        or getattr(exc_cls, "default_code", [])[1:]
        or []
    )
    return ".".join((GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX, *tuple(exc_cls_code)))


# Error codes referenced in docs:
# https://cloud.yandex.ru/docs/datalens/troubleshooting/errors/all
STABILIZED_CODES = (
    (dl_core.exc.CannotParseDateTimeError, "ERR.DS_API.DB.CANNOT_PARSE.DATETIME"),
    (dl_core.exc.CannotParseNumberError, "ERR.DS_API.DB.CANNOT_PARSE.NUMBER"),
    (dl_core.exc.ColumnDoesNotExistError, "ERR.DS_API.DB.COLUMN_DOES_NOT_EXIST"),
    (dl_core.exc.DatabaseQueryError, "ERR.DS_API.DB"),
    (dl_core.exc.DatabaseUnavailableError, "ERR.DS_API.DATABASE_UNAVAILABLE"),
    (dl_core.exc.DbMemoryLimitExceededError, "ERR.DS_API.DB.MEMORY_LIMIT_EXCEEDED"),
    (dl_core.exc.InvalidQueryError, "ERR.DS_API.DB.INVALID_QUERY"),
    (dl_core.exc.JoinColumnTypeMismatchError, "ERR.DS_API.DB.JOIN_COLUMN_TYPE_MISMATCH"),
    (dl_core.exc.MaterializationNotFinishedError, "ERR.DS_API.DB.MATERIALIZATION_NOT_FINISHED"),
    (dl_core.exc.ResultRowCountLimitExceededError, "ERR.DS_API.ROW_COUNT_LIMIT"),
    (dl_core.exc.SourceDoesNotExistError, "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"),
    (dl_core.exc.UnexpectedInfOrNanError, "ERR.DS_API.DB.UNEXPECTED_INF_OR_NAN"),
    (dl_core.exc.TableNameNotConfiguredError, "ERR.DS_API.SOURCE_CONFIG.TABLE_NOT_CONFIGURED"),
    (dl_core.exc.USBadRequestError, "ERR.DS_API.US.BAD_REQUEST"),
    (dl_core.exc.USAlreadyExistsError, "ERR.DS_API.US.BAD_REQUEST.ALREADY_EXISTS"),
    (dl_core.exc.USObjectNotFoundError, "ERR.DS_API.US.OBJ_NOT_FOUND"),
    (dl_core.exc.USAccessDeniedError, "ERR.DS_API.US.ACCESS_DENIED"),
    (dl_core.exc.USWorkbookIsolationInterruptionError, "ERR.DS_API.US.WORKBOOK_ISOLATION_INTERRUPTION"),
    (dl_query_processing.exc.DLFormulaError, "ERR.DS_API.FORMULA"),
    (dl_core.exc.FieldNotFoundError, "ERR.DS_API.FIELD.NOT_FOUND"),
    (dl_query_processing.exc.FilterValueError, "ERR.DS_API.FILTER.INVALID_VALUE"),
    (dl_formula.core.exc.DoubleAggregationError, "ERR.DS_API.VALIDATION.AGG.DOUBLE"),
    (dl_formula.core.exc.InconsistentAggregationError, "ERR.DS_API.VALIDATION.AGG.INCONSISTENT"),
    (dl_formula.core.exc.WindowFunctionWOAggregationError, "ERR.DS_API.VALIDATION.WIN_FUNC.NO_AGG"),
)


def test_exc_code_match():
    for exc_cls, expected_code in STABILIZED_CODES:
        actual_code = exc_cls_to_code(exc_cls)
        assert actual_code == expected_code, exc_cls

from __future__ import annotations

from bi_constants.exc import GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX

import bi_core.exc
import bi_formula.core.exc

import bi_query_processing.exc


def exc_cls_to_code(exc_cls):
    exc_cls_code = (
        getattr(exc_cls, 'err_code', None)
        # formula excs.
        # TODO: document how it is built or use a common function.
        or getattr(exc_cls, 'default_code', [])[1:]
        or []
    )
    return '.'.join(
        (GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX)
        + tuple(exc_cls_code)
    )


# Error codes referenced in the frontend code:
# https://github.yandex-team.ru/data-ui/datalens/blob/52c9d4f52bda30b458c6dd3bfdbba4d2440d8b0c/src/i18n/keysets/ru.json#L2097-L2122
# https://github.yandex-team.ru/data-ui/datalens/blob/64163ae3657d1ecd2651316d29d26a652e21a10f/src/shared/constants/error-codes.ts
# ... and in docs:
# https://bb.yandex-team.ru/projects/CLOUD/repos/docs/browse/ru/datalens/troubleshooting/errors/all.md?useDefaultHandler=true
# https://bb.yandex-team.ru/projects/CLOUD/repos/docs/browse/ru/datalens/troubleshooting/errors
STABILIZED_CODES = (
    (bi_core.exc.CannotParseDateTime, 'ERR.DS_API.DB.CANNOT_PARSE.DATETIME'),
    (bi_core.exc.CannotParseNumber, 'ERR.DS_API.DB.CANNOT_PARSE.NUMBER'),
    (bi_core.exc.ColumnDoesNotExist, 'ERR.DS_API.DB.COLUMN_DOES_NOT_EXIST'),
    (bi_core.exc.DatabaseQueryError, 'ERR.DS_API.DB'),
    (bi_core.exc.DatabaseUnavailable, 'ERR.DS_API.DATABASE_UNAVAILABLE'),
    (bi_core.exc.DbMemoryLimitExceeded, 'ERR.DS_API.DB.MEMORY_LIMIT_EXCEEDED'),
    (bi_core.exc.InvalidQuery, 'ERR.DS_API.DB.INVALID_QUERY'),
    (bi_core.exc.JoinColumnTypeMismatch, 'ERR.DS_API.DB.JOIN_COLUMN_TYPE_MISMATCH'),
    (bi_core.exc.MaterializationNotFinished, 'ERR.DS_API.DB.MATERIALIZATION_NOT_FINISHED'),
    (bi_core.exc.ResultRowCountLimitExceeded, 'ERR.DS_API.ROW_COUNT_LIMIT'),
    (bi_core.exc.SourceDoesNotExist, 'ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST'),
    (bi_core.exc.UnexpectedInfOrNan, 'ERR.DS_API.DB.UNEXPECTED_INF_OR_NAN'),
    (bi_core.exc.TableNameNotConfiguredError, 'ERR.DS_API.SOURCE_CONFIG.TABLE_NOT_CONFIGURED'),
    (bi_core.exc.USBadRequestException, 'ERR.DS_API.US.BAD_REQUEST'),
    (bi_core.exc.USAlreadyExistsException, 'ERR.DS_API.US.BAD_REQUEST.ALREADY_EXISTS'),
    (bi_core.exc.USObjectNotFoundException, 'ERR.DS_API.US.OBJ_NOT_FOUND'),
    (bi_query_processing.exc.DLFormulaError, 'ERR.DS_API.FORMULA'),
    (bi_core.exc.FieldNotFound, 'ERR.DS_API.FIELD.NOT_FOUND'),
    (bi_query_processing.exc.FilterValueError, 'ERR.DS_API.FILTER.INVALID_VALUE'),
    (bi_formula.core.exc.DoubleAggregationError, 'ERR.DS_API.VALIDATION.AGG.DOUBLE'),
    (bi_formula.core.exc.InconsistentAggregationError, 'ERR.DS_API.VALIDATION.AGG.INCONSISTENT'),
    (bi_formula.core.exc.WindowFunctionWOAggregationError, 'ERR.DS_API.VALIDATION.WIN_FUNC.NO_AGG'),
)


def test_exc_code_match():
    for exc_cls, expected_code in STABILIZED_CODES:
        actual_code = exc_cls_to_code(exc_cls)
        assert actual_code == expected_code, exc_cls

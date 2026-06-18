from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)

from dl_connector_chyt.core import exc


def exc_cls_to_code(exc_cls: exc.CHYTQueryError | exc.CHYTCliqueError) -> str:
    return ".".join([GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX, *exc_cls.err_code])


STABILIZED_CODES = (
    (exc.CHYTInvalidSortedJoinError, "ERR.DS_API.DB.CHYT.INVALID_SORTED_JOIN"),
    (exc.CHYTInvalidSortedJoinMoreThanOneTableError, "ERR.DS_API.DB.CHYT.INVALID_SORTED_JOIN.MORE_THAN_ONE_TABLE"),
    (exc.CHYTInvalidSortedJoinNotAKeyColumnError, "ERR.DS_API.DB.CHYT.INVALID_SORTED_JOIN.NOT_A_KEY_COLUMN"),
    (exc.CHYTInvalidSortedJoinNotKeyPrefixColumnError, "ERR.DS_API.DB.CHYT.INVALID_SORTED_JOIN.NOT_KEY_PREFIX_COLUMN"),
    (exc.CHYTMultipleDynamicTablesNotSupportedError, "ERR.DS_API.DB.CHYT.MULTI_DYN_NOT_SUPPORTED"),
    (exc.CHYTQueryError, "ERR.DS_API.DB.CHYT"),
    (exc.CHYTSubqueryWeightLimitExceededError, "ERR.DS_API.DB.CHYT.SUBQ_WEIGHT_LIMIT_EXCEEDED"),
    (exc.CHYTTableAccessDeniedError, "ERR.DS_API.DB.CHYT.TABLE_ACCESS_DENIED"),
    (exc.CHYTCliqueIsNotRunningError, "ERR.DS_API.DB.CHYT.CLIQUE.NOT_RUNNING"),
    (exc.CHYTCliqueIsSuspendedError, "ERR.DS_API.DB.CHYT.CLIQUE.SUSPENDED"),
    (exc.CHYTCliqueNotExistsError, "ERR.DS_API.DB.CHYT.CLIQUE.INVALID_SPECIFICATION"),
    (exc.CHYTCliqueGuidParsingError, "ERR.DS_API.DB.CHYT.CLIQUE.INVALID_GUID"),
    (exc.CHYTCliqueAccessDeniedError, "ERR.DS_API.DB.CHYT.CLIQUE.ACCESS_DENIED"),
    (exc.CHYTTableHasNoSchemaError, "ERR.DS_API.DB.CHYT.TABLE_HAS_NO_SCHEMA"),
    (exc.CHYTAuthError, "ERR.DS_API.DB.CHYT.AUTH_FAILED"),
)


def test_exc_code_match():
    for exc_cls, expected_code in STABILIZED_CODES:
        actual_code = exc_cls_to_code(exc_cls)
        assert actual_code == expected_code, exc_cls

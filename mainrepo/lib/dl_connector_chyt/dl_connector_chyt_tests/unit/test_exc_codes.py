from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)

from dl_connector_chyt.core import exc


def exc_cls_to_code(exc_cls: exc.CHYTQueryError | exc.CHYTCliqueError) -> str:
    return ".".join([GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX] + exc_cls.err_code)


STABILIZED_CODES = (
    (exc.CHYTInvalidSortedJoin, "ERR.DS_API.DB.CHYT.INVALID_SORTED_JOIN"),
    (exc.CHYTInvalidSortedJoinMoreThanOneTable, "ERR.DS_API.DB.CHYT.INVALID_SORTED_JOIN.MORE_THAN_ONE_TABLE"),
    (exc.CHYTInvalidSortedJoinNotAKeyColumn, "ERR.DS_API.DB.CHYT.INVALID_SORTED_JOIN.NOT_A_KEY_COLUMN"),
    (exc.CHYTInvalidSortedJoinNotKeyPrefixColumn, "ERR.DS_API.DB.CHYT.INVALID_SORTED_JOIN.NOT_KEY_PREFIX_COLUMN"),
    (exc.CHYTMultipleDynamicTablesNotSupported, "ERR.DS_API.DB.CHYT.MULTI_DYN_NOT_SUPPORTED"),
    (exc.CHYTQueryError, "ERR.DS_API.DB.CHYT"),
    (exc.CHYTSubqueryWeightLimitExceeded, "ERR.DS_API.DB.CHYT.SUBQ_WEIGHT_LIMIT_EXCEEDED"),
    (exc.CHYTTableAccessDenied, "ERR.DS_API.DB.CHYT.TABLE_ACCESS_DENIED"),
    (exc.CHYTCliqueIsNotRunning, "ERR.DS_API.DB.CHYT.CLIQUE.NOT_RUNNING"),
    (exc.CHYTCliqueIsSuspended, "ERR.DS_API.DB.CHYT.CLIQUE.SUSPENDED"),
    (exc.CHYTCliqueNotExists, "ERR.DS_API.DB.CHYT.CLIQUE.INVALID_SPECIFICATION"),
    (exc.CHYTCliqueAccessDenied, "ERR.DS_API.DB.CHYT.CLIQUE.ACCESS_DENIED"),
    (exc.CHYTTableHasNoSchema, "ERR.DS_API.DB.CHYT.TABLE_HAS_NO_SCHEMA"),
    (exc.CHYTAuthError, "ERR.DS_API.DB.CHYT.AUTH_FAILED"),
)


def test_exc_code_match():
    for exc_cls, expected_code in STABILIZED_CODES:
        actual_code = exc_cls_to_code(exc_cls)
        assert actual_code == expected_code, exc_cls

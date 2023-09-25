from dl_core import exc


class CHYTQueryError(exc.DatabaseQueryError):
    err_code = exc.DatabaseQueryError.err_code + ["CHYT"]
    default_message = "CHYT Error"


class CHYTAuthError(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ["AUTH_FAILED"]
    default_message = "Authentication failed"


class CHYTCliqueError(exc.DatabaseQueryError):
    err_code = CHYTQueryError.err_code + ["CLIQUE"]
    default_message = "Clique Error"


class CHYTCliqueIsNotRunning(CHYTCliqueError):
    err_code = CHYTCliqueError.err_code + ["NOT_RUNNING"]
    default_message = "Clique is not running."
    formatting_messages = {frozenset({"clique"}): "Clique {clique} is not running."}


class CHYTCliqueIsSuspended(CHYTCliqueError):
    err_code = CHYTCliqueError.err_code + ["SUSPENDED"]
    default_message = "Clique is suspended."
    formatting_messages = {frozenset({"clique"}): "Clique {clique} is suspended."}


class CHYTCliqueNotExists(CHYTCliqueError):
    err_code = CHYTCliqueError.err_code + ["INVALID_SPECIFICATION"]
    default_message = "Invalid clique specification. Probably, clique does not exists."


class CHYTCliqueAccessDenied(CHYTCliqueError):
    err_code = CHYTCliqueError.err_code + ["ACCESS_DENIED"]
    default_message = "Access to clique was denied."
    formatting_messages = {frozenset({"clique", "user"}): "Access to clique {clique} for user {user} was denied."}


class CHYTTableHasNoSchema(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ["TABLE_HAS_NO_SCHEMA"]
    default_message = "The table has no schema. Only schematized tables are supported."


class CHYTTableAccessDenied(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ["TABLE_ACCESS_DENIED"]
    default_message = "Access to table was denied."


class CHYTInvalidSortedJoin(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ["INVALID_SORTED_JOIN"]
    default_message = "Invalid sorted JOIN."


class CHYTInvalidSortedJoinNotAKeyColumn(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ["NOT_A_KEY_COLUMN"]
    default_message = "Column used in join expression is not a key column."
    formatting_messages = {frozenset({"col"}): "Column {col} used in join expression is not a key column."}


class CHYTInvalidSortedJoinNotKeyPrefixColumn(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ["NOT_KEY_PREFIX_COLUMN"]
    default_message = "Joined columns should form prefix of joined table key columns."


class CHYTInvalidSortedJoinMoreThanOneTable(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ["MORE_THAN_ONE_TABLE"]
    default_message = (
        "Cannot join a concatenation of tables with another table. " "Wrap concatenation into an SQL query."
    )


class CHYTInvalidSortedJoinTableNotSorted(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ["TABLE_NOT_SORTED"]
    default_message = "Tables should be sorted"
    formatting_messages = {frozenset({"table"}): "Table {table} should be sorted."}


class CHYTInvalidSortedJoinCompoundExpressionsNotSupported(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ["COMPOUND_EXPR_NOT_SUPPORTED"]
    default_message = "CHYT does not support compound expressions in ON/USING clause"


class CHYTInvalidSortedJoinKeyIsEmpty(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ["KEY_IS_EMPTY"]
    default_message = "Cannot join: key is empty"


class CHYTInvalidSortedJoinConcatNotSupported(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ["CONCAT_NOT_SUPPORTED"]
    default_message = "Joining concatenation of multiple tables is not supported"


class CHYTInvalidSortedJoinNotSameKeyPosition(CHYTInvalidSortedJoin):
    err_code = CHYTInvalidSortedJoin.err_code + ["NOT_SAME_KEY_POSITION"]
    default_message = "Joined columns do not occupy same positions in key columns of joined tables"
    formatting_messages = {
        frozenset({"col1", "col2"}): "Joined columns {col1} and {col2}"
        " do not occupy same positions in key columns of joined tables",
    }


class CHYTMultipleDynamicTablesNotSupported(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ["MULTI_DYN_NOT_SUPPORTED"]
    default_message = (
        "Reading multiple dynamic tables " "or dynamic table together with static table is not supported in CHYT"
    )


class CHYTSubqueryWeightLimitExceeded(CHYTQueryError):
    err_code = CHYTQueryError.err_code + ["SUBQ_WEIGHT_LIMIT_EXCEEDED"]
    default_message = "Subquery exceeds data weight limit"

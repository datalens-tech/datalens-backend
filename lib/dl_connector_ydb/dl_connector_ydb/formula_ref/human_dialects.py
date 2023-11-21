from dl_formula_ref.texts import StyledDialect

from dl_connector_ydb.formula.constants import YqlDialect


HUMAN_DIALECTS = {
    YqlDialect.YDB: StyledDialect(
        "`YDB`",
        "`YDB`<br/>(`YQL`)",
        "`YDB` (`YQL`)",
    ),
    YqlDialect.YQ: StyledDialect(
        "`YQ`",
        "`YQ`",
        "`YQ`",
    ),
    YqlDialect.YQL: StyledDialect(
        "`YQL`",
        "`YQL`",
        "`YQL`",
    ),
}

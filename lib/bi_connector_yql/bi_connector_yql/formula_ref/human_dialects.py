from bi_formula_ref.texts import StyledDialect

from bi_connector_yql.formula.constants import YqlDialect


HUMAN_DIALECTS = {
    YqlDialect.YDB: StyledDialect(
        '`YDB`',
        '`YDB`<br/>(`YQL`)',
        '`YDB` (`YQL`)',
    ),
    YqlDialect.YQ: StyledDialect(
        '`YQ`',
        '`YQ`',
        '`YQ`',
    ),
    YqlDialect.YQL: StyledDialect(
        '`YQL`',
        '`YQL`',
        '`YQL`',
    ),
}

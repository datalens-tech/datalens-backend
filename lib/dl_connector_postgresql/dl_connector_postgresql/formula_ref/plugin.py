from dl_formula_ref.functions.array import FUNCTION_UNNEST
from dl_formula_ref.functions.type_conversion import DbCastExtension
from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin
from dl_formula_ref.registry.note import Note

from dl_connector_postgresql.formula.constants import PostgreSQLDialect
from dl_connector_postgresql.formula.definitions.functions_type import FuncDbCastPostgreSQLBase
from dl_connector_postgresql.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_postgresql.formula_ref.i18n import (
    CONFIGS,
    Translatable,
)


class PostgresSQLFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*PostgreSQLDialect.NON_COMPENG_POSTGRESQL.to_list(),))
    compeng_support_dialects = frozenset((*PostgreSQLDialect.POSTGRESQL.to_list(),))  # FIXME: NON_COMPENG_ ?
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
    db_cast_extension = DbCastExtension(
        type_whitelists={
            dialect: whitelist
            for dialect, whitelist in FuncDbCastPostgreSQLBase.WHITELISTS.items()
            if dialect != PostgreSQLDialect.COMPENG
        },
        type_comments={
            (PostgreSQLDialect.NON_COMPENG_POSTGRESQL, "character"): Translatable("Alias: `char`"),
            (PostgreSQLDialect.NON_COMPENG_POSTGRESQL, "character varying"): Translatable("Alias: `varchar`"),
            (PostgreSQLDialect.NON_COMPENG_POSTGRESQL, "char"): Translatable("Alias for `character`"),
            (PostgreSQLDialect.NON_COMPENG_POSTGRESQL, "varchar"): Translatable("Alias for `character varying`"),
        },
    )
    function_extensions = [
        FUNCTION_UNNEST.extend(
            dialect=PostgreSQLDialect.NON_COMPENG_POSTGRESQL,
            notes=(
                Note(
                    Translatable(
                        "{dialects: POSTGRESQL} doesn't allow filtering fields containing the UNNEST "
                        "function. If the data source is {dialects: POSTGRESQL}, do not use such "
                        "fields in selectors."
                    )
                ),
            ),
        ),
    ]

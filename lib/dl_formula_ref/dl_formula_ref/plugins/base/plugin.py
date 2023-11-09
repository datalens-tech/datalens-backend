from typing import ClassVar

from dl_formula.core.dialect import DialectCombo
from dl_formula_ref.config import (
    ConfigVersion,
    RefDocGeneratorConfig,
)
from dl_formula_ref.function_extension import FunctionExtension
from dl_formula_ref.functions.type_conversion import DbCastExtension
from dl_formula_ref.texts import StyledDialect
from dl_i18n.localizer_base import TranslationConfig


class FormulaRefPlugin:
    configs: ClassVar[dict[ConfigVersion, RefDocGeneratorConfig]] = {}
    translation_configs: ClassVar[frozenset[TranslationConfig]] = frozenset()

    # Dialects to include whenever ANY is used instead of implicit dialect list
    any_dialects: ClassVar[frozenset[DialectCombo]] = frozenset()

    # Human-readable dialect names
    human_dialects: ClassVar[dict[DialectCombo, StyledDialect]] = {}

    # Dialects that work with compeng
    compeng_support_dialects: ClassVar[frozenset[DialectCombo]] = frozenset()

    # Connector-specific types and comments for the DB_CAST function
    db_cast_extension: ClassVar[DbCastExtension] = DbCastExtension()

    # Dialect-specific extensions for functions
    function_extensions: list[FunctionExtension] = []

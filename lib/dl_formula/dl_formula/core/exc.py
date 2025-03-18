""" ... """

from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from dl_formula.core.message_ctx import (
    FormulaErrorCtx,
    MessageLevel,
)
from dl_formula.core.position import Position


class ParserNotFoundError(Exception):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.description = (
            "Cannot import parser files,"
            " check if you've generated them with"
            " `${PROJECT_ROOT}/docker_build/run-project-bake gen_antlr`"
        )

    def __str__(self) -> str:
        return self.description


class CacheError(Exception):
    pass


class InspectionError(Exception):
    pass


def _ifnone(first, second):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    return first if first is not None else second


class FormulaError(Exception):
    default_code: ClassVar[Tuple[str, ...]] = ("FORMULA",)

    def __init__(
        self,
        *errors: Union[str, FormulaErrorCtx],
        position: Optional[Position] = None,
        token: Optional[str] = None,
        code: Optional[Sequence[str]] = None,
    ):
        super().__init__()

        self.errors: List[FormulaErrorCtx] = []
        for error in errors:
            if not isinstance(error, FormulaErrorCtx):
                error = FormulaErrorCtx(
                    message=error,
                    level=MessageLevel.ERROR,
                    position=position or Position(),
                    token=token,
                    code=tuple(code or self.default_code or ()),
                )
            elif position is not None or token is not None or code is not None:
                error = FormulaErrorCtx(
                    message=error.message,
                    level=MessageLevel.ERROR,
                    position=_ifnone(position, error.position),
                    token=_ifnone(token, error.token),
                    code=tuple(_ifnone(code, error.code) or self.default_code or ()),
                )

            self.errors.append(error)

        self.error = self.errors[0] if len(errors) == 1 else None
        self.description = "; ".join([str(err) for err in self.errors])

    def __str__(self) -> str:
        return self.description


# Parse Errors


class ParseError(FormulaError):
    default_code = FormulaError.default_code + ("PARSE",)


class ParseUnexpectedEOFError(ParseError):
    default_code = ParseError.default_code + ("UNEXPECTED_EOF",)


class ParseEmptyFormulaError(ParseUnexpectedEOFError):
    default_code = ParseUnexpectedEOFError.default_code + ("EMPTY_FORMULA",)


class ParseUnexpectedTokenError(ParseError):
    default_code = ParseError.default_code + ("UNEXPECTED_TOKEN",)


class ParseValueError(ParseError):
    default_code = ParseError.default_code + ("VALUE",)


class ParseDateValueError(ParseValueError):
    default_code = ParseValueError.default_code + ("DATE",)


class ParseDatetimeValueError(ParseValueError):
    default_code = ParseValueError.default_code + ("DATETIME",)


class ParseClauseError(ParseError):
    default_code = FormulaError.default_code + ("CLAUSE",)


class ParseRecursionError(ParseError):
    default_code = ParseError.default_code + ("RECURSION",)


# Validation Errors


class ValidationError(FormulaError):
    default_code = FormulaError.default_code + ("VALIDATION",)


# Aggregation errors


class AggregationError(ValidationError):
    default_code = ValidationError.default_code + ("AGG",)


class InconsistentAggregationError(AggregationError):
    default_code = AggregationError.default_code + ("INCONSISTENT",)


class DoubleAggregationError(AggregationError):
    default_code = AggregationError.default_code + ("DOUBLE",)


# Window function errors


class WindowFunctionError(ValidationError):
    default_code = ValidationError.default_code + ("WIN_FUNC",)


class NestedWindowFunctionError(WindowFunctionError):
    default_code = WindowFunctionError.default_code + ("NESTED",)


class WindowFunctionWOAggregationError(WindowFunctionError):
    default_code = WindowFunctionError.default_code + ("NO_AGG",)


class WindowFunctionUnselectedDimensionError(WindowFunctionError):
    default_code = WindowFunctionError.default_code + ("BFB_UNSELECTED_DIMENSION",)


# Lookup function errors


class LookupFunctionError(ValidationError):
    default_code = ValidationError.default_code + ("LOOKUP_FUNC",)


class LookupFunctionArgNumberError(LookupFunctionError):
    default_code = LookupFunctionError.default_code + ("ARG_NUM",)


class LookupFunctionUnselectedDimensionError(LookupFunctionError):
    default_code = LookupFunctionError.default_code + ("UNSELECTED_DIMENSION",)


class LookupFunctionIgnoredLookupDimensionError(LookupFunctionError):
    default_code = LookupFunctionError.default_code + ("IGNORED_LOOKUP_DIMENSION",)


class LookupFunctionWOAggregationError(LookupFunctionError):
    default_code = LookupFunctionError.default_code + ("NO_AGG",)


class LookupFunctionConstantLookupDimensionError(LookupFunctionError):
    default_code = LookupFunctionError.default_code + ("CONSTANT_LOOKUP_DIMENSION",)


class LookupFunctionAggregatedDimensionError(LookupFunctionError):
    default_code = LookupFunctionError.default_code + ("AGGREGATED_DIMENSION",)


# LOD errors


class LodError(ValidationError):
    default_code = ValidationError.default_code + ("LOD",)


class LodIncompatibleDimensionsError(LodError):
    default_code = LodError.default_code + ("INCOMPATIBLE_DIMENSIONS",)


class LodInvalidTopLevelDimensionsError(LodError):
    default_code = LodError.default_code + ("INVALID_TOPLEVEL_DIMENSIONS",)


class LodMeasureDimensionsError(LodError):
    default_code = LodError.default_code + ("MEASURE_AS_DIMENSION",)


class UnknownSourceColumnError(FormulaError):
    default_code = FormulaError.default_code + ("UNKNOWN_SOURCE_COLUMN",)


class UnknownFieldInFormulaError(FormulaError):
    default_code = FormulaError.default_code + ("UNKNOWN_FIELD_IN_FORMULA",)


# Translation Errors


class TranslationError(FormulaError):
    default_code = FormulaError.default_code + ("TRANSLATION",)


class DataTypeError(TranslationError):
    default_code = TranslationError.default_code + ("DATA_TYPE",)


class TypeConflictError(DataTypeError):
    default_code = DataTypeError.default_code + ("CONFLICT",)


class TranslationUnknownFunctionError(TranslationError):
    default_code = TranslationError.default_code + ("UNKNOWN_FUNCTION",)


class TranslationUnknownFieldError(TranslationError):
    # An internal error, should not appear to users.
    # If it does, investigate.
    default_code = TranslationError.default_code + ("UNKNOWN_FIELD",)


# Other Errors


class UnknownWindowDimensionError(FormulaError):
    default_code = FormulaError.default_code + ("UNKNOWN_WINDOW_DIMENSION",)

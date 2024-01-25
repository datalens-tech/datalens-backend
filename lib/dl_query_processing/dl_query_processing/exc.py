from __future__ import annotations

from typing import (
    Optional,
    Sequence,
    Union,
)

from dl_constants.exc import DLBaseException
from dl_core.fields import BIField
from dl_formula.core import exc as formula_exc
from dl_formula.core.message_ctx import FormulaErrorCtx
from dl_query_processing.enums import ProcessingStage


class EmptyQuery(DLBaseException):
    err_code = DLBaseException.err_code + ["EMPTY_QUERY"]
    default_message = "Attempted to execute an empty query."


class LogicError(DLBaseException):
    pass


class InvalidGroupByConfiguration(DLBaseException):
    err_code = DLBaseException.err_code + ["INVALID_GROUP_BY_CONFIGURATION"]
    default_message = "Invalid GROUP BY configuration."


class FormulaHandlingError(formula_exc.FormulaError):
    # TODO: Cut off inheritance from FormulaError and patch all errors with field info
    def __init__(
        self,
        *errors: Union[FormulaErrorCtx],
        stage: Optional[ProcessingStage] = None,
        field: Optional[BIField] = None,
    ):
        super().__init__(*errors)
        self.stage = stage
        self.field = field


class InvalidLiteralError(DLBaseException):
    err_code = DLBaseException.err_code + ["INVALID_VALUE"]
    default_message = "Invalid value for literal"


class FilterError(DLBaseException):
    err_code = DLBaseException.err_code + ["FILTER"]
    default_message = "Invalid filter"


class FilterValueError(FilterError):
    err_code = FilterError.err_code + ["INVALID_VALUE"]
    default_message = "Invalid value for filter"


class MeasureFilterUnsupportedError(FilterError):
    err_code = FilterError.err_code + ["MEASURE_UNSUPPORTED"]
    default_message = "Measure filter is unsupported for this type of query"


class FilterArgumentCountError(FilterError):
    err_code = FilterError.err_code + ["ARGUMENT_COUNT_ERROR"]
    default_message = "Invalid argument count for filter"


class ParameterError(DLBaseException):
    err_code = DLBaseException.err_code + ["PARAMETER"]
    default_message = "Invalid parameter"


class ParameterValueError(ParameterError):
    err_code = ParameterError.err_code + ["INVALID_VALUE"]
    default_message = "Invalid value for parameter"


class ParameterUnsupportedTypeError(ParameterError):
    err_code = ParameterError.err_code + ["UNSUPPORTED"]
    default_message = "Unsupported type for parameter"


class InvalidQueryStructure(DLBaseException):
    err_code = DLBaseException.err_code + ["INVALID_QUERY_STRUCTURE"]
    default_message = "Failed to compile data query."


class BlockSpecError(DLBaseException):
    err_code = DLBaseException.err_code + ["BLOCK"]
    default_message = "Block spec error."


class MultipleBlocksUnsupportedError(BlockSpecError):
    err_code = BlockSpecError.err_code + ["MULTIPLE_UNSUPPORTED"]
    default_message = "This API does not support multiple query blocks."


class UnevenBlockColumnCountError(BlockSpecError):
    err_code = BlockSpecError.err_code + ["UNEVEN_COLUMN_COUNT"]
    default_message = "Blocks have different column count."


class NoRootBlockError(BlockSpecError):
    err_code = BlockSpecError.err_code + ["NO_ROOT"]
    default_message = "Got no blocks with root placement."


class MultipleRootBlockError(BlockSpecError):
    err_code = BlockSpecError.err_code + ["MULTIPLE_ROOTS"]
    default_message = "Got more than one block with root placement."


class BlockItemCompatibilityError(BlockSpecError):
    err_code = BlockSpecError.err_code + ["ITEM_COMPATIBILITY"]
    default_message = "Got items with incompatible roles in block."


class TreeError(DLBaseException):
    err_code = DLBaseException.err_code + ["TREE"]


class MultipleTreeError(TreeError):
    err_code = TreeError.err_code + ["MULTIPLE"]


class LegendError(DLBaseException):
    err_code = DLBaseException.err_code + ["LEGEND"]


class LegendItemReferenceError(LegendError):
    err_code = LegendError.err_code + ["ITEM_REFERENCE"]


class UnsopportedRoleInLegend(LegendError):
    err_code = LegendError.err_code + ["UNSUPPORTED_ROLE"]


class RoleDataTypeMismatch(LegendError):
    err_code = LegendError.err_code + ["ROLE_DATA_TYPE_MISMATCH"]


class NonUniqueLegendIdsError(LegendError):
    err_code = LegendError.err_code + ["NON_UNIQUE_IDS"]


class LegendMeasureNameError(LegendError):
    err_code = LegendError.err_code + ["MEASURE_NAME"]
    default_message = "Measure names misconfigured."


class MeasureNameUnsupported(LegendMeasureNameError):
    err_code = LegendMeasureNameError.err_code + ["UNSUPPORTED"]
    default_message = "Measure names is not supported."


class GenericInvalidRequestError(DLBaseException):
    err_code = DLBaseException.err_code + ["INVALID_REQUEST"]
    default_message = "Invalid request"


class PivotError(DLBaseException):
    err_code = DLBaseException.err_code + ["PIVOT"]
    default_message = "Pivot error."


class PivotLegendError(PivotError):
    err_code = PivotError.err_code + ["LEGEND"]


class PivotLegendItemReferenceError(PivotLegendError):
    err_code = PivotLegendError.err_code + ["ITEM_REFERENCE"]


class PivotSortingError(PivotError):
    err_code = PivotError.err_code + ["SORTING"]
    default_message = "Pivot sorting error."


class PivotSortingMultipleColumnsOrRows(PivotSortingError):
    err_code = PivotSortingError.err_code + ["MULTIPLE_COLUMNS_OR_ROWS"]
    default_message = "Requested sort by multiple columns or multiple rows."


class PivotSortingAgainstMultipleMeasures(PivotSortingError):
    err_code = PivotSortingError.err_code + ["AGAINST_MULTIPLE_MEASURES"]
    default_message = "If there are multiple measures, sorting can only be done along them."


class PivotSortingRowOrColumnNotFound(PivotSortingError):
    err_code = PivotSortingError.err_code + ["ROW_OR_COLUMN_NOT_FOUND"]
    default_message = "Requested sorting row or column not found."


class PivotSortingRowOrColumnIsAmbiguous(PivotSortingError):
    err_code = PivotSortingError.err_code + ["ROW_OR_COLUMN_IS_AMBIGUOUS"]
    default_message = "More than one row or column match requested sorting settings."


class PivotSortingWithSubtotalsIsNotAllowed(PivotSortingError):
    err_code = PivotSortingError.err_code + ["SUBTOTALS_ARE_NOT_ALLOWED"]
    default_message = "Measure sorting with subtotals is not allowed."


class PivotMeasureNameError(PivotError):
    err_code = PivotError.err_code + ["MEASURE_NAME"]
    default_message = "Pivot measure names error."


class PivotMeasureNameRequired(PivotMeasureNameError):
    err_code = PivotMeasureNameError.err_code + ["REQUIRED"]
    default_message = "Measure names must be used when pivoting multiple measures."


class PivotMeasureNameForbidden(PivotMeasureNameError):
    err_code = PivotMeasureNameError.err_code + ["FORBIDDEN"]
    default_message = "Measure names are not allowed in pivot table without measures."


class PivotMeasureNameDuplicate(PivotMeasureNameError):
    err_code = PivotMeasureNameError.err_code + ["DUPLICATE"]
    default_message = "Measure names cannot be used twice in pivot table."


class PivotInvalidRoleLegendError(PivotLegendError):
    err_code = PivotLegendError.err_code + ["INVALID_ROLE"]


class PivotDimensionError(PivotError):
    err_code = PivotError.err_code + ["DIMENSION"]
    default_message = "Pivot dimension error."


class PivotDimensionDuplicate(PivotDimensionError):
    err_code = PivotDimensionError.err_code + ["DUPLICATE"]
    default_message = "Pivot dimension cannot be used twice."


class PivotDuplicateDimensionValue(PivotDimensionError):
    err_code = PivotDimensionError.err_code + ["DUPLICATE_VALUE"]
    default_message = "Duplicate dimension values were encountered in pivot table."


class PivotOverflowError(PivotError):
    err_code = PivotError.err_code + ["OVERFLOW"]
    default_message = "Pivot table size overflow error."


class PivotUnevenDataColumnsError(PivotError):
    err_code = PivotError.err_code + ["UNEVEN_DATA_COLUMNS"]
    default_message = "Data columns for pivot table have uneven sizes."


class PivotItemsIncompatibleError(PivotLegendError):
    err_code = PivotLegendError.err_code + ["ITEMS_INCOMPATIBLE"]


class UnresolvableSlicingTagOrder(DLBaseException):
    err_code = DLBaseException.err_code + ["UNRESOLVABLE_TAG_ORDER"]
    default_message = "Function nesting order could not be resolved"  # FIXME


class DLFormulaError(DLBaseException):
    # TODO: catch all cases of FormulaError in DatasetView and re-raise as this exception
    err_code = DLBaseException.err_code + ["FORMULA"]

    def __init__(
        self,
        message: Optional[str] = None,
        details: Optional[dict] = None,
        orig: Exception = None,  # type: ignore  # 2024-01-24 # TODO: Incompatible default for argument "orig" (default has type "None", argument has type "Exception")  [assignment]
        field: Optional[BIField] = None,
        formula_errors: Optional[Sequence[FormulaErrorCtx]] = None,
    ):
        details = {} if details is None else details.copy()
        if field is not None:
            details["field"] = {"guid": field.guid, "title": field.title, "errors": []}
        sub_errors = []
        for err in formula_errors or ():
            sub_errors.append(
                {
                    "code": err.code,
                    "message": err.message,
                    "row": err.position.start_row,
                    "col": err.position.start_col,
                    "token": err.token,
                }
            )
        details.update(formula_errors=sub_errors)

        super().__init__(message=message, details=details, orig=orig)


class DatasetError(DLBaseException):
    err_code = DLBaseException.err_code + ["DATASET"]


class DatasetTooManyFieldsFatal(DatasetError):
    err_code = DatasetError.err_code + ["TOO_MANY_FIELDS"]
    default_message = "There are too many fields in the dataset"

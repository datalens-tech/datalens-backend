from __future__ import annotations

import json
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Optional,
    Sequence,
)

from dl_constants.enums import BIType
from dl_formula.core.datatype import DataType
from dl_query_processing.postprocessing.postprocessors.datetime import (
    make_postprocess_datetimetz,
    postprocess_datetime,
    postprocess_genericdatetime,
)
from dl_query_processing.postprocessing.postprocessors.geo import (
    postprocess_geopoint,
    postprocess_geopolygon,
)
from dl_query_processing.postprocessing.postprocessors.markup import postprocess_markup
from dl_query_processing.postprocessing.primitives import PostprocessedData


if TYPE_CHECKING:
    from dl_query_processing.compilation.primitives import DetailedType  # type: ignore  # TODO: fix


def stringify_or_null(value: Any) -> Optional[str]:
    if value is None:
        return value
    return str(value)


def postprocess_array(value: Optional[Iterable[Any]]) -> Optional[Iterable[Optional[str]]]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)  # Frontend fully supports non-ASCII JSON


TYPE_PROCESSORS = {
    BIType.datetime: postprocess_datetime,
    # parametrized: BIType.datetimetz
    BIType.genericdatetime: postprocess_genericdatetime,
    BIType.geopoint: postprocess_geopoint,
    BIType.geopolygon: postprocess_geopolygon,
    BIType.markup: postprocess_markup,
    BIType.array_int: postprocess_array,
    BIType.array_float: postprocess_array,
    BIType.array_str: postprocess_array,
    BIType.tree_str: postprocess_array,
}


def get_type_processor(field_type_info: Optional[DetailedType]) -> Callable[[Any], Any]:
    if field_type_info is None:
        return lambda val: val

    # Basic
    result = TYPE_PROCESSORS.get(field_type_info.data_type)
    if result is not None:
        return result  # type: ignore  # TODO: fix

    # Parmetrized
    if field_type_info.data_type == BIType.datetimetz:
        assert field_type_info.formula_data_type == DataType.DATETIMETZ
        assert field_type_info.formula_data_type_params
        assert field_type_info.formula_data_type_params.timezone
        return make_postprocess_datetimetz(field_type_info.formula_data_type_params.timezone)

    # Default
    return stringify_or_null


# TODO FIX: Turn into generator when response streaming become real
def postprocess_data(
    data: Iterable[Sequence[Any]],
    field_types: Iterable[Optional[DetailedType]],
) -> PostprocessedData:
    columns_processors = [get_type_processor(ft) for ft in field_types]

    return tuple(tuple(processor(col) for processor, col in zip(columns_processors, row)) for row in data)

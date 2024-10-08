from datetime import date

import pytest

from dl_constants.enums import UserDataType
from dl_core.fields import (
    BIField,
    DirectCalculationSpec,
    ResultSchema,
)
from dl_formula.core import nodes
from dl_formula.shortcuts import n
from dl_query_processing.compilation.primitives import CompiledFormulaInfo
from dl_query_processing.enums import ExecutionLevel

from dl_connector_bitrix_gds.api.multi_query import BitrixGDSMultiQuerySplitter


RESULT_SCHEMA = ResultSchema(
    [
        BIField.make(
            title="valid_date",
            guid="valid_date",
            calc_spec=DirectCalculationSpec(source="some_date_source"),
            data_type=UserDataType.date,
        ),
        BIField.make(
            title="user_date",
            guid="user_date",
            calc_spec=DirectCalculationSpec(source="UF_CRM_source"),
            data_type=UserDataType.date,
        ),
        BIField.make(
            title="string",
            guid="string",
            calc_spec=DirectCalculationSpec(source="some_string_source"),
            data_type=UserDataType.string,
        ),
    ]
)


@pytest.mark.parametrize(
    "formula_obj, original_field_id, expected",
    [
        pytest.param(
            n.formula(
                n.ternary(
                    "between",
                    first=n.field("valid_date"),
                    second=n.lit(date.today()),
                    third=n.lit(date.today()),
                )
            ),
            "valid_date",
            True,
            id="between filter on a date field",
        ),
        pytest.param(
            n.formula(
                n.binary(
                    "==",
                    left=n.field("valid_date"),
                    right=n.lit(date.today()),
                )
            ),
            "valid_date",
            True,
            id="== filter on a date field",
        ),
        pytest.param(
            n.formula(
                n.binary(
                    "==",
                    left=n.field("user_date"),
                    right=n.lit(date.today()),
                )
            ),
            "user_date",
            False,
            id="filter on a user date field",
        ),
        pytest.param(
            n.formula(
                n.binary(
                    "==",
                    left=n.field("string"),
                    right=n.lit("lol"),
                ),
            ),
            "string",
            False,
            id="filter on a string field",
        ),
        pytest.param(
            n.formula(
                n.binary(
                    "==",
                    left=n.field("valid_date"),
                    right=n.lit(date.today()),
                )
            ),
            None,
            False,
            id="filter on a date field without the original field id",
        ),
        pytest.param(
            n.formula(
                n.binary(
                    "==",
                    left=n.func.DATEADD(n.field("valid_date")),
                    right=n.lit(date.today()),
                ),
            ),
            "valid_date",
            True,  # FIXME BI-5836: Bitrix dialect doesn't have the DATEADD function
            id="filter on DATEADD([valid_date])",
        ),
    ],
)
def test_pre_filter(formula_obj: nodes.Formula, original_field_id: str | None, expected: bool) -> None:
    splitter = BitrixGDSMultiQuerySplitter(crop_to_level_type=ExecutionLevel.compeng, result_schema=RESULT_SCHEMA)
    formula = CompiledFormulaInfo(
        alias="filter",
        formula_obj=formula_obj,
        original_field_id=original_field_id,
    )
    assert splitter.is_pre_filter(formula) == expected

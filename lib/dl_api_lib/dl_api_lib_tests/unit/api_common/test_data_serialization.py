from types import SimpleNamespace

import pytest

from dl_api_lib.api_common.data_serialization import (
    get_fields_data_raw,
    get_fields_data_serializable,
)
from dl_constants.enums import (
    AggregationFunction,
    CalcMode,
    FieldType,
    ManagedBy,
    UserDataType,
)
from dl_core.fields import (
    BIField,
    DirectCalculationSpec,
    FormulaCalculationSpec,
)


def _make_dataset(*fields):
    return SimpleNamespace(result_schema=list(fields))


def _make_user_field(
    *,
    guid: str,
    title: str,
    calc_spec=None,
    aggregation: AggregationFunction = AggregationFunction.none,
    description: str = "",
    managed_by: ManagedBy = ManagedBy.user,
    cast: UserDataType = UserDataType.string,
):
    if calc_spec is None:
        calc_spec = DirectCalculationSpec(source=title, avatar_id=None)

    return BIField.make(
        guid=guid,
        title=title,
        calc_spec=calc_spec,
        aggregation=aggregation,
        type=FieldType.DIMENSION,
        hidden=False,
        description=description,
        cast=cast,
        initial_data_type=UserDataType.string,
        data_type=UserDataType.string,
        valid=True,
        managed_by=managed_by,
        ui_settings="",
    )


_BASE_KEYS = {"title", "guid", "data_type", "cast", "calc_mode", "hidden", "type", "ui_settings"}
_DETAIL_KEYS = {"description", "formula", "aggregation"}


def test_default_does_not_include_details():
    direct_field = _make_user_field(
        guid="g_direct",
        title="Direct",
        description="some desc",
        aggregation=AggregationFunction.sum,
    )
    formula_field = _make_user_field(
        guid="g_formula",
        title="Formula",
        calc_spec=FormulaCalculationSpec(formula="[Direct] + 1", guid_formula="[g_direct] + 1"),
        description="formula desc",
    )
    dataset = _make_dataset(direct_field, formula_field)

    fields = get_fields_data_raw(dataset)

    assert len(fields) == 2
    for field in fields:
        assert set(field.keys()) == _BASE_KEYS


def test_for_result_strips_ui_attributes():
    field = _make_user_field(guid="g", title="Direct")
    dataset = _make_dataset(field)

    fields = get_fields_data_raw(dataset, for_result=True)

    assert set(fields[0].keys()) == {"title", "guid", "data_type", "cast", "calc_mode"}


def test_include_details_adds_exactly_three_keys():
    direct_field = _make_user_field(
        guid="g_direct",
        title="Direct",
        description="direct desc",
        aggregation=AggregationFunction.sum,
    )
    formula_field = _make_user_field(
        guid="g_formula",
        title="Formula",
        calc_spec=FormulaCalculationSpec(formula="[Direct] + 1", guid_formula="[g_direct] + 1"),
        description="formula desc",
        aggregation=AggregationFunction.none,
    )
    dataset = _make_dataset(direct_field, formula_field)

    fields = get_fields_data_raw(dataset, include_details=True)

    assert len(fields) == 2
    for field in fields:
        assert set(field.keys()) == _BASE_KEYS | _DETAIL_KEYS

    by_guid = {field["guid"]: field for field in fields}
    assert by_guid["g_direct"]["description"] == "direct desc"
    assert by_guid["g_direct"]["formula"] == ""
    assert by_guid["g_direct"]["aggregation"] is AggregationFunction.sum

    assert by_guid["g_formula"]["description"] == "formula desc"
    assert by_guid["g_formula"]["formula"] == "[Direct] + 1"
    assert by_guid["g_formula"]["aggregation"] is AggregationFunction.none


def test_include_details_handles_empty_values():
    direct_field = _make_user_field(guid="g", title="Direct")
    dataset = _make_dataset(direct_field)

    fields = get_fields_data_raw(dataset, include_details=True)

    assert fields[0]["description"] == ""
    assert fields[0]["formula"] == ""
    assert fields[0]["aggregation"] is AggregationFunction.none


def test_cast_present_in_raw_and_serializable():
    field = _make_user_field(guid="g", title="Direct", cast=UserDataType.integer)
    dataset = _make_dataset(field)

    raw = get_fields_data_raw(dataset)
    assert raw[0]["cast"] is UserDataType.integer

    serializable = get_fields_data_serializable(dataset)
    assert serializable[0]["cast"] == UserDataType.integer.name


def test_managed_by_filter_unaffected_by_include_details():
    user_field = _make_user_field(guid="g_user", title="User", managed_by=ManagedBy.user)
    feature_field = _make_user_field(guid="g_feature", title="Feature", managed_by=ManagedBy.feature)
    dataset = _make_dataset(user_field, feature_field)

    plain = get_fields_data_raw(dataset)
    detailed = get_fields_data_raw(dataset, include_details=True)

    assert [field["guid"] for field in plain] == ["g_user"]
    assert [field["guid"] for field in detailed] == ["g_user"]


@pytest.mark.parametrize("include_details", [False, True])
def test_serializable_dumps_known_keys(include_details: bool):
    direct_field = _make_user_field(
        guid="g_direct",
        title="Direct",
        description="d",
        aggregation=AggregationFunction.sum,
    )
    formula_field = _make_user_field(
        guid="g_formula",
        title="Formula",
        calc_spec=FormulaCalculationSpec(formula="[Direct]", guid_formula="[g_direct]"),
    )
    dataset = _make_dataset(direct_field, formula_field)

    fields = get_fields_data_serializable(dataset, include_details=include_details)

    expected_keys = _BASE_KEYS | (_DETAIL_KEYS if include_details else set())
    for field in fields:
        assert set(field.keys()) == expected_keys
        assert field["calc_mode"] in {CalcMode.direct.name, CalcMode.formula.name}

    if include_details:
        by_guid = {field["guid"]: field for field in fields}
        assert by_guid["g_direct"]["aggregation"] == AggregationFunction.sum.name
        assert by_guid["g_formula"]["formula"] == "[Direct]"

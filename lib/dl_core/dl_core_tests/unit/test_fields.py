from dl_constants.enums import ParameterValueConstraintType
from dl_core.fields import (
    AllParameterValueConstraint,
    BIField,
    RangeParameterValueConstraint,
    SetParameterValueConstraint,
)
from dl_model_tools.schema.typed_values import (
    IntegerValue,
    StringValue,
)
from dl_testing.utils import get_log_record


def test_rename_in_formula(caplog):
    map = {"apples": "oranges"}
    formula = "COUNTD([apples]) + COUNTD([bananas]) + 1"
    assert BIField.rename_in_formula(formula, map) == "COUNTD([oranges]) + COUNTD([bananas]) + 1"

    log_record = get_log_record(caplog, predicate=lambda r: r.message.startswith("Unknown field"), single=True)
    assert log_record.message == "Unknown field: bananas"


def test_all_parameter_value_constraint():
    constraint = AllParameterValueConstraint()
    assert constraint.type == ParameterValueConstraintType.all
    assert constraint.is_valid(StringValue("foo"))
    assert constraint.is_valid(IntegerValue(42))


def test_set_parameter_value_constraint():
    constraint = SetParameterValueConstraint(values=[StringValue("foo"), StringValue("bar")])
    assert constraint.type == ParameterValueConstraintType.set
    assert constraint.is_valid(StringValue("foo"))
    assert constraint.is_valid(StringValue("bar"))
    assert not constraint.is_valid(StringValue("baz"))


def test_range_parameter_value_constraint():
    constraint = RangeParameterValueConstraint(min=IntegerValue(1), max=IntegerValue(5))
    assert constraint.type == ParameterValueConstraintType.range
    assert not constraint.is_valid(IntegerValue(0))
    assert constraint.is_valid(IntegerValue(1))
    assert constraint.is_valid(IntegerValue(2))
    assert constraint.is_valid(IntegerValue(5))
    assert not constraint.is_valid(IntegerValue(6))

from dl_constants.enums import ParameterValueConstraintType
from dl_core.fields import (
    BIField,
    CollectionParameterValueConstraint,
    DefaultParameterValueConstraint,
    EqualsParameterValueConstraint,
    NotEqualsParameterValueConstraint,
    NullParameterValueConstraint,
    RangeParameterValueConstraint,
    RegexParameterValueConstraint,
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
    constraint = NullParameterValueConstraint()
    assert constraint.type == ParameterValueConstraintType.null
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


def test_equals_parameter_value_constraint():
    constraint = EqualsParameterValueConstraint(value=IntegerValue(42))
    assert constraint.type == ParameterValueConstraintType.equals
    assert constraint.is_valid(IntegerValue(42))
    assert not constraint.is_valid(IntegerValue(43))


def test_not_equals_parameter_value_constraint():
    constraint = NotEqualsParameterValueConstraint(value=IntegerValue(42))
    assert constraint.type == ParameterValueConstraintType.not_equals
    assert not constraint.is_valid(IntegerValue(42))
    assert constraint.is_valid(IntegerValue(43))


def test_regex_parameter_value_constraint():
    constraint = RegexParameterValueConstraint(pattern="foo.*")
    assert constraint.type == ParameterValueConstraintType.regex
    assert constraint.is_valid(StringValue("foo"))
    assert constraint.is_valid(StringValue("foobar"))
    assert not constraint.is_valid(StringValue("bar"))
    assert not constraint.is_valid(StringValue("barfoo"))

    constraint = RegexParameterValueConstraint(pattern=".*bar")
    assert constraint.type == ParameterValueConstraintType.regex
    assert constraint.is_valid(StringValue("bar"))
    assert constraint.is_valid(StringValue("foobar"))
    assert not constraint.is_valid(StringValue("foo"))
    assert not constraint.is_valid(StringValue("barfoo"))

    constraint = RegexParameterValueConstraint(pattern="^.*$")
    assert constraint.type == ParameterValueConstraintType.regex
    assert constraint.is_valid(StringValue("foo"))


def test_default_parameter_value_constraint():
    constraint = DefaultParameterValueConstraint()
    assert constraint.type == ParameterValueConstraintType.default
    assert constraint.is_valid(StringValue("foo"))
    assert constraint.is_valid(StringValue("BaR"))
    assert constraint.is_valid(IntegerValue(42))

    for char in "!@#$%^&*()_+-=[]{}|;':\",.<>?/":
        assert not constraint.is_valid(StringValue(char))


def test_collection_parameter_value_constraint():
    constraint = CollectionParameterValueConstraint(
        constraints=[
            NotEqualsParameterValueConstraint(value=IntegerValue(42)),
            NotEqualsParameterValueConstraint(value=IntegerValue(43)),
        ]
    )

    assert constraint.type == ParameterValueConstraintType.collection
    assert constraint.is_valid(IntegerValue(44))
    assert not constraint.is_valid(IntegerValue(42))
    assert not constraint.is_valid(IntegerValue(43))

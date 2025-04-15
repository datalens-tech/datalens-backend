import pytest

from dl_api_lib.schemas.parameters import ParameterValueConstraintSchema
from dl_constants.enums import ParameterValueConstraintType
from dl_model_tools.schema.typed_values import VALUE_TYPE_CONTEXT_KEY


@pytest.mark.parametrize(
    "data, expected_type",
    [
        ({"type": "null"}, ParameterValueConstraintType.null),
        ({"type": "range", "min": 1, "max": 5}, ParameterValueConstraintType.range),
        ({"type": "set", "values": [1, 2]}, ParameterValueConstraintType.set),
        ({"type": "equals", "value": 42}, ParameterValueConstraintType.equals),
        ({"type": "not_equals", "value": 42}, ParameterValueConstraintType.not_equals),
        ({"type": "regex", "pattern": ".*"}, ParameterValueConstraintType.regex),
        ({"type": "default"}, ParameterValueConstraintType.default),
    ],
)
def test_parameter_value_constraint_schema(data: dict, expected_type: ParameterValueConstraintType):
    schema = ParameterValueConstraintSchema(
        context={VALUE_TYPE_CONTEXT_KEY: "integer"},
    )
    obj = schema.load(data)
    assert obj.type == expected_type
    assert schema.dump(obj) == data


def test_parameter_value_collection_constraint_schema():
    schema = ParameterValueConstraintSchema(
        context={VALUE_TYPE_CONTEXT_KEY: "integer"},
    )
    data = {
        "type": "collection",
        "constraints": [
            {"type": "null"},
            {"type": "not_equals", "value": 42},
        ],
    }
    obj = schema.load(data)
    assert obj.type == ParameterValueConstraintType.collection
    assert schema.dump(obj) == data

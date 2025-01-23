import pytest

from dl_api_lib.schemas.parameters import ParameterValueConstraintSchema
from dl_constants.enums import ParameterValueConstraintType
from dl_model_tools.schema.typed_values import VALUE_TYPE_CONTEXT_KEY


@pytest.mark.parametrize(
    "data, expected_type",
    [
        ({"type": "all"}, ParameterValueConstraintType.all),
        ({"type": "range", "min": 1, "max": 5}, ParameterValueConstraintType.range),
        ({"type": "set", "values": [1, 2]}, ParameterValueConstraintType.set),
    ],
)
def test_parameter_value_constraint_schema(data: dict, expected_type: ParameterValueConstraintType):
    schema = ParameterValueConstraintSchema(
        context={VALUE_TYPE_CONTEXT_KEY: "integer"},
    )
    obj = schema.load(data)
    assert obj.type == expected_type
    assert schema.dump(obj) == data

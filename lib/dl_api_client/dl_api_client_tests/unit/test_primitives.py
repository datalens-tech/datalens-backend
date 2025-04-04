from dl_api_client.dsmaker.primitives import (
    JoinCondition,
    ResultField,
    ResultFieldJoinPart,
)
from dl_constants.enums import (
    BinaryJoinOperator,
    CalcMode,
    UserDataType,
)


# FIXME: This is practically a dummy. Add tests please


def test_field_condition_operators() -> None:
    field_1 = ResultField(
        title="field_1",
        id="12345",
        data_type=UserDataType.string,
        initial_data_type=UserDataType.string,
        cast=UserDataType.string,
        calc_mode=CalcMode.direct,
        source="column_1",
    )
    field_2 = ResultField(
        title="field_2",
        id="67890",
        data_type=UserDataType.string,
        initial_data_type=UserDataType.string,
        cast=UserDataType.string,
        calc_mode=CalcMode.direct,
        source="column_2",
    )
    condition = field_1 == field_2
    assert condition == JoinCondition(
        left_part=ResultFieldJoinPart(field_id=field_1.id),
        right_part=ResultFieldJoinPart(field_id=field_2.id),
        operator=BinaryJoinOperator.eq,
    )

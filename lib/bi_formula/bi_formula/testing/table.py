import datetime
from typing import Any, NamedTuple

from sqlalchemy.types import TypeEngine

from bi_formula.core.datatype import DataType
from bi_formula.core.dialect import DialectCombo
from bi_formula.connectors.base.type_constructor import get_type_constructor


DAY = datetime.timedelta(days=1)
START_DATE = datetime.date(2014, 10, 5)
START_DATETIME = datetime.datetime(2014, 10, 5)
DATETIME_DELTA = datetime.timedelta(days=1, hours=1, minutes=10, seconds=30)


class ColumnSpec(NamedTuple):
    name: str
    data_type: DataType
    primary_key: bool = False


TABLE_SPEC = (
    ColumnSpec(name='id', data_type=DataType.INTEGER, primary_key=True),
    ColumnSpec(name='int_value', data_type=DataType.INTEGER),
    ColumnSpec(name='date_value', data_type=DataType.DATE),
    ColumnSpec(name='datetime_value', data_type=DataType.DATETIME),
    ColumnSpec(name='str_value', data_type=DataType.STRING),
    ColumnSpec(name='str_null_value', data_type=DataType.STRING),
)


TABLE_SPEC_ARRAYS = (
    ColumnSpec(name='arr_int_value', data_type=DataType.ARRAY_INT),
    ColumnSpec(name='arr_float_value', data_type=DataType.ARRAY_FLOAT),
    ColumnSpec(name='arr_str_value', data_type=DataType.ARRAY_STR),
)


NULL_DATA_TABLE_SPEC = (
    ColumnSpec(name='int_null', data_type=DataType.INTEGER),
    ColumnSpec(name='float_null', data_type=DataType.FLOAT),
    ColumnSpec(name='bool_null', data_type=DataType.BOOLEAN),
    ColumnSpec(name='str_null', data_type=DataType.STRING),
    ColumnSpec(name='date_null', data_type=DataType.DATE),
    ColumnSpec(name='datetime_null', data_type=DataType.DATETIME),
    ColumnSpec(name='geopoint_null', data_type=DataType.GEOPOINT),
    ColumnSpec(name='geopolygon_null', data_type=DataType.GEOPOLYGON),
    ColumnSpec(name='uuid_null', data_type=DataType.UUID),
)


def generate_sample_data(
        start_date: datetime.date = START_DATE,
        start_datetime: datetime.datetime = START_DATETIME,
        datetime_delta: datetime.timedelta = DATETIME_DELTA,
        add_arrays: bool = False,
) -> list[dict[str, Any]]:
    data = []
    for num, idx in enumerate([*range(10), *range(10)]):
        row = dict(
            id=num + 1,
            int_value=idx * 10,
            date_value=start_date + DAY * idx,
            datetime_value=start_datetime + datetime_delta * idx,
            str_value='q' * (idx+1),
            str_null_value=None,
        )
        if add_arrays:
            row.update(
                arr_int_value=[1 * idx, 23 + idx, 456, None],
                arr_float_value=[1.23 * idx, 45.0 + idx, 0.123, None],
                arr_str_value=['a' * idx, 'bb' * idx, 'cde', None],
            )
        data.append(row)
    return data


def get_column_sa_type(data_type: DataType, dialect: DialectCombo) -> TypeEngine:
    type_constructor = get_type_constructor(dialect_name=dialect.common_name)
    return type_constructor.get_sa_type(data_type)

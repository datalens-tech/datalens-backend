import sqlalchemy_bigquery._types as bq_types

from dl_constants.enums import UserDataType
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    make_native_type,
)


class BigQueryTypeTransformer(TypeTransformer):
    native_to_user_map = {
        make_native_type(bq_types.DATE): UserDataType.date,
        make_native_type(bq_types.DATETIME): UserDataType.genericdatetime,
        make_native_type(bq_types.STRING): UserDataType.string,
        make_native_type(bq_types.BOOLEAN): UserDataType.boolean,
        make_native_type(bq_types.INTEGER): UserDataType.integer,
        make_native_type(bq_types.FLOAT): UserDataType.float,
        make_native_type(bq_types.NUMERIC): UserDataType.float,
        make_native_type(bq_types.ARRAY(bq_types.INTEGER)): UserDataType.array_int,
        make_native_type(bq_types.ARRAY(bq_types.STRING)): UserDataType.array_str,
        make_native_type(bq_types.ARRAY(bq_types.FLOAT)): UserDataType.array_float,
    }
    user_to_native_map = {
        UserDataType.date: make_native_type(bq_types.DATE),
        UserDataType.genericdatetime: make_native_type(bq_types.DATETIME),
        UserDataType.string: make_native_type(bq_types.STRING),
        UserDataType.boolean: make_native_type(bq_types.BOOLEAN),
        UserDataType.integer: make_native_type(bq_types.INTEGER),
        UserDataType.float: make_native_type(bq_types.FLOAT),
        UserDataType.array_int: make_native_type(bq_types.ARRAY(bq_types.INTEGER)),
        UserDataType.array_str: make_native_type(bq_types.ARRAY(bq_types.STRING)),
        UserDataType.array_float: make_native_type(bq_types.ARRAY(bq_types.FLOAT)),
    }

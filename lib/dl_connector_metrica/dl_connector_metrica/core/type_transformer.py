from dl_constants.enums import UserDataType
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    make_native_type,
)


class MetrikaApiTypeTransformer(TypeTransformer):
    native_to_user_map = {
        make_native_type("string"): UserDataType.string,
        make_native_type("integer"): UserDataType.integer,
        make_native_type("float"): UserDataType.float,
        make_native_type("date"): UserDataType.date,
        make_native_type("datetime"): UserDataType.genericdatetime,
    }

    user_to_native_map = {
        UserDataType.string: make_native_type("string"),
        UserDataType.integer: make_native_type("integer"),
        UserDataType.float: make_native_type("float"),
        UserDataType.date: make_native_type("date"),
        UserDataType.datetime: make_native_type("datetime"),
        UserDataType.genericdatetime: make_native_type("datetime"),
        UserDataType.datetimetz: make_native_type("datetime"),
    }

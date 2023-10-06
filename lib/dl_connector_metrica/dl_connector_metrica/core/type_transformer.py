from dl_constants.enums import UserDataType
from dl_core.db import TypeTransformer
from dl_core.db.conversion_base import make_native_type

from dl_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API


class MetrikaApiTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_METRICA_API

    native_to_user_map = {
        make_native_type(CONNECTION_TYPE_METRICA_API, "string"): UserDataType.string,
        make_native_type(CONNECTION_TYPE_METRICA_API, "integer"): UserDataType.integer,
        make_native_type(CONNECTION_TYPE_METRICA_API, "float"): UserDataType.float,
        make_native_type(CONNECTION_TYPE_METRICA_API, "date"): UserDataType.date,
        make_native_type(CONNECTION_TYPE_METRICA_API, "datetime"): UserDataType.genericdatetime,
    }

    user_to_native_map = {
        UserDataType.string: make_native_type(CONNECTION_TYPE_METRICA_API, "string"),
        UserDataType.integer: make_native_type(CONNECTION_TYPE_METRICA_API, "integer"),
        UserDataType.float: make_native_type(CONNECTION_TYPE_METRICA_API, "float"),
        UserDataType.date: make_native_type(CONNECTION_TYPE_METRICA_API, "date"),
        UserDataType.datetime: make_native_type(CONNECTION_TYPE_METRICA_API, "datetime"),
        UserDataType.genericdatetime: make_native_type(CONNECTION_TYPE_METRICA_API, "datetime"),
        UserDataType.datetimetz: make_native_type(CONNECTION_TYPE_METRICA_API, "datetime"),
    }

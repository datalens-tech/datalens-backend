from __future__ import annotations

from bi_constants.enums import BIType, ConnectionType

from bi_core.db.conversion_base import TypeTransformer, make_native_type


class MonitoringTypeTransformer(TypeTransformer):
    conn_type = ConnectionType.monitoring
    native_to_user_map = {
        make_native_type(ConnectionType.monitoring, 'datetime'): BIType.genericdatetime,
        make_native_type(ConnectionType.monitoring, 'float'): BIType.float,
        make_native_type(ConnectionType.monitoring, 'string'): BIType.string,
    }
    user_to_native_map = dict([
        (bi_type, native_type) for native_type, bi_type in native_to_user_map.items()
    ] + [
        (BIType.datetime, make_native_type(ConnectionType.monitoring, 'datetime')),
    ])

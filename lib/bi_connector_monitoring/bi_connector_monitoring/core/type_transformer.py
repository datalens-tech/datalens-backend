from __future__ import annotations

from dl_constants.enums import BIType

from dl_core.db.conversion_base import TypeTransformer, make_native_type

from bi_connector_monitoring.core.constants import CONNECTION_TYPE_MONITORING


class MonitoringTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_MONITORING
    native_to_user_map = {
        make_native_type(CONNECTION_TYPE_MONITORING, 'datetime'): BIType.genericdatetime,
        make_native_type(CONNECTION_TYPE_MONITORING, 'float'): BIType.float,
        make_native_type(CONNECTION_TYPE_MONITORING, 'string'): BIType.string,
    }
    user_to_native_map = dict([
        (bi_type, native_type) for native_type, bi_type in native_to_user_map.items()
    ] + [
        (BIType.datetime, make_native_type(CONNECTION_TYPE_MONITORING, 'datetime')),
    ])

from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_api_lib import exc
from dl_api_lib.api_decorators import schematic_request
from dl_api_lib.app.control_api.resources.base import BIResource
from dl_api_lib.app.control_api.resources.connections import ns
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.utils import need_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler
from dl_core.exc import ConnectionConfigurationError
from dl_core.us_connection_base import ConnectionBase
from dl_model_tools.schema.base import BaseSchema
from dl_sqlalchemy_metrica_api.exceptions import MetrikaApiAccessDeniedException

from dl_connector_metrica.core.constants import (
    CONNECTION_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API,
)
from dl_connector_metrica.core.us_connection import MetrikaBaseMixin


class MetricaConnectionAvailableCountersSchema(BaseSchema):
    class CounterInfo(BaseSchema):
        id = ma_fields.String()
        name = ma_fields.String()

    counters = ma_fields.Nested(CounterInfo, many=True)


@ns.route("/<connection_id>/metrica_available_counters")
class MetricaConnectionAvailableCounters(BIResource):
    @schematic_request(ns=ns, responses={200: ("Success", MetricaConnectionAvailableCountersSchema())})
    @generic_profiler("metrica-available-counters")
    def get(self, connection_id: str) -> dict:
        conn = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)
        need_permission_on_entry(conn, USPermissionKind.edit)
        if conn.conn_type not in (CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API):
            raise exc.UnsupportedForEntityType("Unsupported connection type")
        assert isinstance(conn, MetrikaBaseMixin)
        try:
            counters = conn.get_available_counters()
        except MetrikaApiAccessDeniedException as ex:
            raise ConnectionConfigurationError(
                "Unable to load available counters. Possibly caused by invalid OAuth token."
            ) from ex
        else:
            return dict(counters=counters)

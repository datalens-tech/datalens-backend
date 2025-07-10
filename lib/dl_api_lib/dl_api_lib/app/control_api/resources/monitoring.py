from __future__ import annotations

import logging
from typing import (
    Any,
    ClassVar,
    NoReturn,
)

from dl_api_commons.flask.required_resources import RequiredResourceCommon
from dl_api_lib.app.control_api.resources import API
from dl_api_lib.app.control_api.resources.base import BIResource
from dl_constants.exc import DLBaseException
from dl_core.exc import DatabaseUnavailable


LOGGER = logging.getLogger(__name__)

ns = API.namespace("Monitoring", path="/monitoring", description="Monitoring endpoints")


def _handle_monitoring_exc(exception: Exception) -> NoReturn:
    if isinstance(exception, DLBaseException):
        raise exception
    else:
        LOGGER.exception("Got unhandled monitoring exception")
        raise DatabaseUnavailable("Service monitoring failed") from exception


class MonitoringResourceBase(BIResource):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResourceCommon]] = frozenset(
        {RequiredResourceCommon.SKIP_AUTH, RequiredResourceCommon.US_HEADERS_TOKEN}
    )


@ns.route("/ping")
class PingResource(MonitoringResourceBase):
    def get(self) -> Any:
        """Simple ping endpoint to check service availability"""
        try:
            return {"status": "ok"}
        except Exception as e:
            _handle_monitoring_exc(e)


@ns.route("/unistat")
class UnistatResource(MonitoringResourceBase):
    def get(self) -> Any:
        """Endpoint for service statistics"""
        try:
            return {"status": "ok"}
        except Exception as e:
            _handle_monitoring_exc(e)


@ns.route("/metrics")
class MetricsResource(MonitoringResourceBase):
    def get(self) -> Any:
        """Prometheus metrics endpoint"""
        try:
            return {"status": "ok"}
        except Exception as e:
            _handle_monitoring_exc(e)

from __future__ import annotations

from collections import OrderedDict
import logging
import os
from typing import (
    Any,
    ClassVar,
    NoReturn,
)

from flask import (
    Response,
    make_response,
)
from statcommons.unistat.common import (
    dump_for_prometheus,
    results_to_response,
)
from statcommons.unistat.uwsgi import (
    uwsgi_prometheus,
    uwsgi_unistat,
)

from dl_api_commons.flask.required_resources import RequiredResourceCommon
from dl_api_lib.app.control_api.resources import API
from dl_api_lib.app.control_api.resources.base import BIResource
from dl_constants.exc import DLBaseException
from dl_core.exc import DatabaseUnavailable


LOGGER = logging.getLogger(__name__)

ns = API.namespace("Monitoring", path="/", description="Monitoring endpoints")


def _handle_monitoring_exc(exception: Exception) -> NoReturn:
    if isinstance(exception, DLBaseException):
        raise exception
    else:
        LOGGER.exception("Got unhandled monitoring exception")
        raise DatabaseUnavailable("Service monitoring failed") from exception


class MonitoringResourceBase(BIResource):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResourceCommon]] = frozenset({RequiredResourceCommon.SKIP_AUTH})


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
            results = OrderedDict()
            if os.environ.get("UWSGI_STATS") and os.environ.get("QLOUD_APPLICATION"):
                results.update(uwsgi_unistat())

            data = "".join(results_to_response(results))
            return Response(data, content_type="application/json; charset=utf-8")
        except Exception as e:
            _handle_monitoring_exc(e)


@ns.route("/metrics")
class MetricsResource(MonitoringResourceBase):
    def get(self) -> Any:
        """Prometheus metrics endpoint"""
        try:
            data = []
            if os.environ.get("UWSGI_STATS") and os.environ.get("QLOUD_APPLICATION"):
                data = uwsgi_prometheus()

            body = "".join(dump_for_prometheus(data))
            return make_response(body)
        except Exception as e:
            _handle_monitoring_exc(e)

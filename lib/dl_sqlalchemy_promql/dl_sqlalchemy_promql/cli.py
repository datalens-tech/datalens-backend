from __future__ import annotations

from datetime import datetime
import logging
from urllib.parse import urljoin

import requests
from requests.packages.urllib3.util import Retry

from .errors import (
    InterfaceError,
    NotSupportedError,
)


LOGGER = logging.getLogger(__name__)


def rebuild_prometheus_data(data):
    result = []
    schema = None
    for chunk in data["result"]:
        chunk_schema = [("timestamp", "unix_timestamp"), ("value", "float64")] + [
            (label, "string") for label in chunk["metric"]
        ]
        if schema is None:
            schema = chunk_schema
        elif set(schema) != set(chunk_schema):
            raise NotSupportedError("Different schemas are not supported")

        rows = [
            {**chunk["metric"], **dict(timestamp=datetime.fromtimestamp(ts), value=float(v))}
            for (ts, v) in chunk["values"]
        ]
        result.append((schema, rows))

    return result


class SyncPromQLClient:
    def __init__(self, base_url, username, password, **kwargs):
        self._closed = False
        self._session = requests.Session()
        self._session.max_redirects = 0
        for prefix in ("http://", "https://"):
            self._session.mount(
                prefix,
                requests.adapters.HTTPAdapter(
                    max_retries=Retry(
                        total=5,
                        backoff_factor=0.1,
                        status_forcelist=(500, 501, 502, 503, 504, 521),
                        method_whitelist=frozenset(["GET", "POST"]),
                    )
                ),
            )
        if username and password:
            auth = requests.auth.HTTPBasicAuth(username, password)
            self._session = auth(self._session)
        self._base_url = base_url

    def close(self):
        self._closed = True
        self._session.close()

    def _request(self, method, endpoint, **kwargs):
        if endpoint.startswith("/"):
            LOGGER.warning(f"Endpoint '{endpoint}' starts with '/' that can effect final url")
        url = urljoin(self._base_url, endpoint)
        return self._session.request(method, url, **kwargs)

    def query_range(self, query, parameters):
        req_params = {"start", "end", "step"}
        if parameters is None or not req_params <= set(parameters):
            raise InterfaceError("'step', 'start', 'end' must be in parameters")

        # TODO: query parametrization
        response = self._request(
            "post",
            "api/v1/query_range",
            data={
                "query": query,
                "start": parameters["start"],
                "end": parameters["end"],
                "step": parameters["step"],
            },
        )
        response.raise_for_status()
        data = response.json()
        result = rebuild_prometheus_data(data["data"])

        return result

    def test_connection(self):
        response = self._request("get", "-/ready")
        response.raise_for_status()

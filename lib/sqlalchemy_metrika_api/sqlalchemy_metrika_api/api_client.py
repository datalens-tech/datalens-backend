from __future__ import annotations

import datetime
from json.decoder import JSONDecodeError
import logging
from typing import (
    List,
    Optional,
)

import requests
from requests.exceptions import RequestException
from sqlalchemy_metrika_api.exceptions import (  # noqa
    ConnectionClosedException,
    CursorClosedException,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    MetrikaApiAccessDeniedException,
    MetrikaApiException,
    MetrikaApiObjectNotFoundException,
    MetrikaHttpApiException,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)


LOGGER = logging.getLogger(__name__)


METRIKA_API_HOST = "https://api-metrika.yandex.net"
APPMETRICA_API_HOST = "https://api.appmetrica.yandex.ru"


def _get_retriable_requests_session():
    session = requests.Session()
    for schema in ["http://", "https://"]:
        session.mount(
            schema,
            # noinspection PyUnresolvedReferences
            requests.adapters.HTTPAdapter(
                max_retries=requests.packages.urllib3.util.Retry(
                    total=5,
                    backoff_factor=0.5,
                    status_forcelist=[500, 501, 502, 504, 521],
                    redirect=10,
                    method_whitelist=frozenset(["HEAD", "TRACE", "GET", "PUT", "OPTIONS", "DELETE", "POST"]),
                ),
            ),
        )
    # TODO: allow customize UA
    ua = "{}, sqlalchemy-metrika_api".format(requests.utils.default_user_agent())
    session.headers.update({"User-Agent": ua})
    return session


def _parse_metrika_error(response):
    msg = "Unknown error"
    try:
        resp_data = response.json()
        msg = resp_data["message"]
    except Exception:
        LOGGER.exception("Unable to fetch error message.")
    return msg


class MetrikaApiClient(object):
    """
    Simple HTTP client for Metrika API
    https://tech.yandex.ru/metrika/doc/api2/api_v1/intro-docpage/
    """

    host = METRIKA_API_HOST
    default_timeout = 60

    def __init__(self, oauth_token: str, host: Optional[str] = None, default_timeout=-1, **kwargs):
        if host is not None:
            self.host = host
        if default_timeout != -1:
            self.default_timeout = default_timeout
        self.oauth_token = oauth_token
        self._session = _get_retriable_requests_session()
        self._session.headers.update({"Authorization": "OAuth {}".format(oauth_token)})

    @property
    def _is_appmetrica(self):
        return self.host == APPMETRICA_API_HOST

    def _request(self, method: str, uri: str, timeout: int = -1, _raw_resp: bool = False, **kwargs):
        if timeout == -1:
            timeout = self.default_timeout
        full_url = "/".join(map(lambda s: s.strip("/"), (self.host, uri)))

        LOGGER.info(
            "Requesting Metrika API: method: %s, url: %s, params:(%s), json:(%s)",
            method,
            full_url,
            kwargs.get("params", {}),
            kwargs.get("json", {}),
        )

        response = None
        try:
            response = self._session.request(
                method,
                full_url,
                timeout=timeout,
                allow_redirects=False,
                **kwargs,
            )
            LOGGER.info(
                "Got %s from Metrika API (%s %s), content length: %s",
                response.status_code,
                method,
                uri,
                response.headers.get("Content-Length"),
            )
            if response.status_code >= 400:
                LOGGER.error(
                    "Metrika API error on %s %s (%s): %s",
                    method,
                    uri,
                    kwargs.get("json", {}),
                    response.text,
                )
            # TODO: wrap 429 and maybe retry
            if _raw_resp:
                return response

            response.raise_for_status()
        except RequestException as ex:
            msg = _parse_metrika_error(response)
            if response.status_code == 403:
                raise MetrikaApiAccessDeniedException(msg, orig_exc=ex) from ex
            elif response.status_code == 404:
                raise MetrikaApiObjectNotFoundException(msg, orig_exc=ex) from ex
            else:
                raise MetrikaHttpApiException(msg, orig_exc=ex) from ex

        try:
            parsed_resp = response.json()
        except JSONDecodeError as ex:
            raise MetrikaHttpApiException("Unable to parse response.", orig_exc=ex)
        return parsed_resp

    def get(self, uri, **kwargs):
        return self._request("GET", uri, **kwargs)

    def post(self, uri, **kwargs):
        return self._request("POST", uri, **kwargs)

    def _parse_data_resp(self, resp, result_columns=None, req_metrics=None):
        """
        https://tech.yandex.ru/metrika/doc/api2/api_v1/data-docpage/
        """
        LOGGER.info(
            "Received data response: total_rows: %s, sample_share: %s", resp["total_rows"], resp["sample_share"]
        )

        rows = []
        try:
            q_metrics = resp["query"]["metrics"]
            q_dims = resp["query"]["dimensions"]
            if req_metrics is not None and len(req_metrics) == 1:
                req_metrics = req_metrics[0].split(",")
                if len(q_metrics) != len(req_metrics):
                    raise MetrikaApiException("Unexpected response metrics count.")
                if q_metrics != req_metrics:
                    LOGGER.info(
                        "Response query metrics not matching requested metrics: %s, %s.",
                        q_metrics,
                        req_metrics,
                    )
                    q_metrics = req_metrics

            rc_dict = {col["name"]: col for col in result_columns}
            dims_src_keys = [rc_dict.get(dim, {}).get("src_key") or "name" for dim in q_dims]

            if not result_columns:
                result_columns = [dict(name=col_name, label=None) for col_name in (*q_dims, *q_metrics)]

            for slice in resp["data"]:
                # TODO: date dimensions values better retrieve from 'id' key instead of 'name'?
                row_map = {q_dims[i]: dim_item[dims_src_keys[i]] for i, dim_item in enumerate(slice["dimensions"])}
                row_map.update({q_metrics[i]: metr_val for i, metr_val in enumerate(slice["metrics"])})
                rows.append(
                    tuple(
                        col["cast_processor"](row_map[col["name"]]) if "cast_processor" in col else row_map[col["name"]]
                        for col in result_columns
                    )
                )
        except (KeyError, ValueError) as ex:
            raise MetrikaApiException(orig_exc=ex)

        return dict(
            fields=result_columns,
            data=rows,
        )

    def get_table_data(self, params, result_columns=None, **kwargs):
        resp = self.get("/stat/v1/data", params=params, **kwargs)
        return self._parse_data_resp(
            resp,
            result_columns=result_columns,
            req_metrics=params.get("metrics"),
        )

    def get_available_counters(self, **kwargs) -> List[dict]:
        obj_name = "applications" if self._is_appmetrica else "counters"
        uri = "/management/v1/{}".format(obj_name)
        resp = self.get(uri, **kwargs)
        return [dict(id=c_info["id"], name=c_info["name"]) for c_info in resp[obj_name]]

    def get_counter_info(self, counter_id):
        """
        https://tech.yandex.ru/metrika/doc/api2/management/counters/counter-docpage/
        """
        obj_name = "application" if self._is_appmetrica else "counter"
        uri = "/management/v1/{obj_name}/{counter_id}".format(obj_name=obj_name, counter_id=counter_id)
        resp = self.get(uri)
        return resp[obj_name]

    def get_counter_creation_date(self, counter_id) -> datetime.date:
        counter_info = self.get_counter_info(counter_id)
        try:
            date_str = counter_info.get("create_time", counter_info.get("create_date")).split("T")[0]
            creation_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, KeyError) as ex:
            raise MetrikaApiException(orig_exc=ex)
        return creation_date

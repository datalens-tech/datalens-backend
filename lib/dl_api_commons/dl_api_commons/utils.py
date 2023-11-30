import requests
import requests.adapters

from dl_constants.api_constants import (
    DLCookies,
    DLHeaders,
)


def stringify_dl_headers(headers: dict[DLHeaders, str]) -> dict[str, str]:
    return {name.value.lower(): val for name, val in headers.items()}


def stringify_dl_cookies(headers: dict[DLCookies, str]) -> dict[str, str]:
    return {name.value: val for name, val in headers.items()}


def get_requests_session() -> requests.Session:
    session = requests.Session()
    ua = "{}, Datalens".format(requests.utils.default_user_agent())
    session.headers.update({"User-Agent": ua})
    return session


def get_retriable_requests_session() -> requests.Session:
    session = get_requests_session()

    retry_conf = requests.adapters.Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 501, 502, 503, 504, 521],
        redirect=10,
        method_whitelist=frozenset(["HEAD", "TRACE", "GET", "PUT", "OPTIONS", "DELETE", "POST"]),
        # # TODO:
        # # (the good: will return a response when it's an error response)
        # # (the bad: need to raise_for_status() manually, same as without retry conf)
        # raise_on_status=False,
    )

    for schema in ("http://", "https://"):
        session.mount(
            schema,
            # noinspection PyUnresolvedReferences
            requests.adapters.HTTPAdapter(
                max_retries=retry_conf,
            ),
        )

    return session

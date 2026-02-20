from typing import Protocol

import httpx

import dl_constants
import dl_retrier
import dl_utils


class RetryRequestMutator(Protocol):
    def on_retry(self, request: httpx.Request, retry: dl_retrier.Retry) -> None:
        ...


class RequestIdRetryMutator:
    _ORIGINAL_ID_KEY = "_original_request_id"
    _REQUEST_ID_HEADER = dl_constants.DLHeadersCommon.REQUEST_ID.value

    def on_retry(self, request: httpx.Request, retry: dl_retrier.Retry) -> None:
        if self._ORIGINAL_ID_KEY not in request.extensions:
            request.extensions[self._ORIGINAL_ID_KEY] = request.headers.get(self._REQUEST_ID_HEADER)
        original_id = request.extensions[self._ORIGINAL_ID_KEY]
        if original_id is not None:
            request.headers[self._REQUEST_ID_HEADER] = dl_utils.append_retry_suffix(
                original_id,
                retry.attempt_number,
            )

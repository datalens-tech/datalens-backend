import contextlib
import json
import logging
from typing import (
    Any,
    Iterator,
    NoReturn,
    Optional,
)

import attr
import marshmallow.exceptions

from dl_api_commons.client.base import (
    DLCommonAPIClient,
    Req,
    Resp,
)
import dl_api_commons.exc

LOGGER = logging.getLogger(__name__)


@attr.s()
class CommonInternalAPIClient(DLCommonAPIClient):
    _use_workbooks_api: bool = attr.ib(default=False)
    _read_only: bool = attr.ib(default=False)

    async def make_request(self, rq: Req) -> Resp:
        if self._read_only:
            if rq.method.lower() != "get":
                raise AssertionError(
                    f"Can not execute {rq.method!r} {rq.url!r} due to {type(self).__name__!r} is read-only"
                )

        return await super().make_request(rq)

    @contextlib.contextmanager
    def deserialization_err_handler(self, resp: Resp, op_code: str) -> Iterator[None]:
        try:
            yield None
        except marshmallow.exceptions.ValidationError as ma_exc:
            LOGGER.info(f"Deserialization fail in internal API client: {type(self).__name__}", exc_info=True)
            effective_messages: dict[str, Any] = ma_exc.messages  # type: ignore

            raise dl_api_commons.exc.MalformedAPIResponseErr(
                self.create_exc_data(
                    resp,
                    operation=op_code,
                    validation_errors=effective_messages,
                )
            ) from ma_exc

    def create_exc_data(
        self, resp: Resp, operation: str, validation_errors: Optional[dict[str, Any]] = None
    ) -> dl_api_commons.exc.APIResponseData:
        body: Any
        try:
            body = resp.json
        except Exception:  # noqa
            body = resp.content

        return dl_api_commons.exc.APIResponseData(
            status_code=resp.status,
            response_body=body,
            operation=operation,
            response_body_validation_errors=validation_errors,
        )

    def raise_from_resp(self, resp: Resp, *, op_code: str) -> NoReturn:
        common_data = self.create_exc_data(resp, op_code)
        resp_data: str
        try:
            resp_data = json.dumps(common_data.response_body)
        except Exception:  # noqa
            resp_data = repr(common_data.response_body)

        LOGGER.info(
            f"Handling unexpected response in internal API client: {type(self).__name__}",
            extra=dict(
                status_code=resp.status,
                resp_data=resp_data,
            ),
        )

        if resp.status in (401, 403):
            raise dl_api_commons.exc.AccessDeniedErr(common_data)
        if resp.status == 404:
            raise dl_api_commons.exc.NotFoundErr(common_data)

        raise dl_api_commons.exc.InternalAPIBadResponseErr(common_data)

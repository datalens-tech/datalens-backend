from typing import Optional

import aiohttp
import attr
from dl_api_commons.base_models import TenantDef

from bi_integration_tests import report_formatting
from bi_testing_ya import api_wrappers, cloud_tokens


@attr.s(auto_attribs=True, frozen=True)
class BaseRequestExecutor:
    client: api_wrappers.APIClient
    logger: report_formatting.ReportFormatter

    @classmethod
    def from_settings(
        cls,
        base_url: str,
        folder_id: str,
        logger: report_formatting.ReportFormatter,
        account_creds: cloud_tokens.AccountCredentials | None = None,
        public_api_key: str | None = None,
        tenant: Optional[TenantDef] = None
    ):
        """Creates executor object using passed parameters."""
        web_app = api_wrappers.HTTPClientWrapper(
            session=aiohttp.ClientSession(),
            base_url=base_url
        )

        client = api_wrappers.APIClient(
            web_app=web_app,
            folder_id=folder_id,
            account_credentials=account_creds,
            public_api_key=public_api_key,
            tenant=tenant
        )

        return cls(client, logger)

    async def _request(self, request: api_wrappers.Req) -> api_wrappers.Resp:
        """Executes request to the API and reports logs."""
        response = None
        try:
            response = await self.client.make_request(request)
        except api_wrappers.RequestExecutionException as e:
            # for logging we need only status and request id
            if request.require_ok:
                raise
            response = api_wrappers.Resp(
                status=e.status,
                content=b"",
                json={},
                req_id=e.request_id,
                content_type=None,
                headers={},
            )
        finally:
            # it is generally unexpected, but we can have type of exception different from RequestExecutionException.
            # in this case, response will be empty and there is no need to log it additionally.
            if response:
                self.logger.log_request(request=request, response=response)

        return response


class WaitTimeoutError(Exception):
    pass


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class ConnectionSourceData:
    id: str
    title: str


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class FileConnectionSourceData(ConnectionSourceData):
    file_id: str


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class ConnectionData:
    name: str
    dir_path: str
    type: str
    sources: list[ConnectionSourceData]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class FileConnectionData(ConnectionData):
    type: str = attr.ib(default="file", init=False)
    sources: list[FileConnectionSourceData]


__all__ = [
    "BaseRequestExecutor",
    "WaitTimeoutError",
    "ConnectionSourceData",
    "FileConnectionSourceData",
    "ConnectionData",
    "FileConnectionData",
]

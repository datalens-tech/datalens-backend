from __future__ import annotations

import logging
from typing import (
    Any,
    Optional,
)

import attr

# https://a.yandex-team.ru/arc/trunk/arcadia/cloud/bitbucket/private-api/yandex/cloud/priv/oauth/v1/session_service.proto
from yandex.cloud.priv.oauth.v1 import (
    session_service_pb2,
    session_service_pb2_grpc,
)

from bi_cloud_integration.local_metadata import get_yc_service_token_local
from bi_cloud_integration.yc_client_base import DLYCSingleServiceClient
from bi_cloud_integration.yc_ts_client import get_yc_service_token
from dl_utils.aio import await_sync

LOGGER = logging.getLogger(__name__)


@attr.s
class DLSSClient(DLYCSingleServiceClient):
    """DataLens-specific client for YC Session Service"""

    service_cls = session_service_pb2_grpc.SessionServiceStub

    async def check(self, cookie_header: str, host: str) -> Any:  # session_service_pb2.CheckSessionResponse:
        if not self.bearer_token:
            raise ValueError("`bearer_token` must be filled here. Use `cli = await cli.ensure_fresh_token()`")
        request = session_service_pb2.CheckSessionRequest(cookie_header=cookie_header, host=host)
        ss_resp = await self.service.Check.aio(request)
        # # Example ss_resp at 2021-02-20T15:45:31+03:00:
        # subject_claims {
        #   sub: "bfb…1tp"
        #   name: "An…ev"
        #   preferred_username: "hh…eam.ru"
        #   picture: "https://avatars.mds.yandex.net/get-yapic/0/0-0/islands-middle"
        #   email: "hh…eam.ru"
        #   federation {
        #     id: "yc.yandex-team.federation"
        #     name: "yandex-cloud-federation"
        #   }
        # }
        # expires_at {
        #   seconds: 1614771949  # 2021-03-03T14:45:49+03:00
        #   nanos: 32091000
        # }
        # cloud_user_info {
        #   is_eula_accepted: true
        #   is_member_of_cloud: true
        #   create_cloud_restrictions {
        #     has_email: true
        #   }
        # }
        # iam_token {
        #   iam_token: "t1.9eu…sDw"
        #   expires_at {
        #     seconds: 1613864864  # 2021-02-21T02:47:44+03:00
        #     nanos: 697261000
        #   }
        # }
        return ss_resp

    def check_sync(self, cookie_header: str, host: str) -> Any:  # session_service_pb2.CheckSessionResponse:
        return await_sync(self.check(cookie_header=cookie_header, host=host))

    async def ensure_fresh_token(
        self,
        sa_key_data: Optional[dict[str, str]] = None,
        ts_endpoint: Optional[str] = None,
    ) -> DLSSClient:
        # TODO: Check current token expiration time and don't fetch new token every time.
        if sa_key_data is not None:
            assert ts_endpoint is not None
            token = await get_yc_service_token(
                key_data=sa_key_data,
                yc_ts_endpoint=ts_endpoint,
            )
        else:
            token = await get_yc_service_token_local()
        return self.clone(bearer_token=token)

    def ensure_fresh_token_sync(self) -> DLSSClient:
        return await_sync(self.ensure_fresh_token())

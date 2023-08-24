from __future__ import annotations

import json
import time
from typing import Any, Optional, Union

import jwt

from yandex.cloud.priv.iam.v1.iam_token_service_pb2 import (
    CreateIamTokenForServiceAccountRequest, CreateIamTokenRequest,
)
from yandex.cloud.priv.iam.v1.iam_token_service_pb2_grpc import IamTokenServiceStub

from bi_utils.aio import await_sync

from .local_metadata import get_yc_service_token_local
from .yc_client_base import DLYCSingleServiceClient, DLYCRetryPolicy


class DLTSClient(DLYCSingleServiceClient):
    """ TS (Token Service) client """

    service_cls = IamTokenServiceStub

    @staticmethod
    def jwt_encode_token(
            service_account_id: str, key_id: str, private_key: str,
            expiration: int = 360, timestamp: Optional[float] = None,
    ) -> str:
        if timestamp is None:
            timestamp = time.time()
        timestamp = int(timestamp)
        payload = dict(
            aud='https://iam.api.cloud.yandex.net/iam/v1/tokens',
            iss=service_account_id,  # e.g. bfbks94685cfqsau06r0
            iat=timestamp,
            exp=timestamp + expiration,
        )
        return jwt.encode(
            payload,
            private_key.encode('ascii'),  # '-----BEGIN PRIVATE KEY-----\n...\n...--END ...---'
            algorithm='PS256',
            headers={'kid': key_id},  # e.g. 'bfbmvosk975v4duo8pke'
        )

    async def create_token_resp(
            self, service_account_id: str, key_id: str, private_key: str,
            expiration: int = 360, timestamp: Optional[float] = None,
    ) -> Any:
        encoded_token = self.jwt_encode_token(
            service_account_id=service_account_id, key_id=key_id,
            private_key=private_key, expiration=expiration,
            timestamp=timestamp,
        )
        req = CreateIamTokenRequest(jwt=encoded_token)
        return await self.service.Create.aio(req)

    def create_token_resp_sync(
            self, service_account_id: str, key_id: str, private_key: str,
            expiration: int = 360, timestamp: Optional[float] = None,
    ) -> Any:
        return await_sync(self.create_token_resp(
            service_account_id=service_account_id, key_id=key_id, private_key=private_key,
            expiration=expiration, timestamp=timestamp,
        ))

    async def create_token(
            self, service_account_id: str, key_id: str, private_key: str,
            expiration: int = 360, timestamp: Optional[float] = None,
    ) -> str:
        rs = await self.create_token_resp(
            service_account_id=service_account_id, key_id=key_id,
            private_key=private_key, expiration=expiration,
            timestamp=timestamp,
        )
        return rs.iam_token

    async def authenticate_by_oauth_resp(self, oauth_token: str) -> Any:
        req = CreateIamTokenRequest(yandex_passport_oauth_token=oauth_token)
        rs = await self.service.Create.aio(req)
        return rs

    def authenticate_by_oauth_resp_sync(self, oauth_token: str) -> Any:
        return await_sync(self.authenticate_by_oauth_resp(oauth_token))

    async def ensure_fresh_token(self) -> DLTSClient:
        token = await get_yc_service_token_local()
        return self.clone(bearer_token=token)

    async def create_iam_token_for_service_account(self, service_account_id: str) -> str:
        if not self.bearer_token:
            raise ValueError("`bearer_token` must be filled here. Use `cli = await cli.ensure_fresh_token()`")
        req = CreateIamTokenForServiceAccountRequest(service_account_id=service_account_id)
        resp = await self.service.CreateForServiceAccount.aio(req)
        return resp.iam_token


async def get_yc_service_token(
        key_data: Union[bytes, str, dict], yc_ts_endpoint: str,
        timeout: float = 32.0, expiration: int = 3600) -> str:
    """
    :param key_data: dict / json string, with IAM service account auth key data

        {"service_account_id": "...", "key_id": "...", "private_key": "..."}

    """
    if isinstance(key_data, (bytes, str)):
        key_data = json.loads(key_data)
    if not isinstance(key_data, dict):
        raise ValueError("Unexpected key_data", key_data)

    yc_ts_client = DLTSClient.create(
        endpoint=yc_ts_endpoint,
        retry_policy=DLYCRetryPolicy(
            total_timeout=timeout,
            call_timeout=timeout * 0.9,
        ),
    )
    iam_token = await yc_ts_client.create_token(
        service_account_id=key_data['service_account_id'],
        key_id=key_data.get('key_id') or key_data['id'],
        private_key=key_data['private_key'],
        expiration=expiration,
    )
    return iam_token


def get_yc_service_token_sync(
        key_data: Union[bytes, str, dict], yc_ts_endpoint: str,
        timeout: float = 32.0, expiration: int = 3600) -> str:
    return await_sync(get_yc_service_token(
        key_data=key_data, yc_ts_endpoint=yc_ts_endpoint,
        timeout=timeout, expiration=expiration))

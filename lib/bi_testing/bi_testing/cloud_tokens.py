from __future__ import annotations

import datetime
import json
import os
from typing import Optional

import attr
import grpc

from bi_cloud_integration.yc_ts_client import DLTSClient


@attr.s(frozen=True)
class ServiceAccountAndKeyData:
    sa_id: str = attr.ib()
    key_id: str = attr.ib()
    key_pem_data: str = attr.ib(repr=False)


@attr.s
class AccountCredentials:
    user_id: str = attr.ib()
    token: str = attr.ib(repr=False)
    is_intranet_user: bool = attr.ib(default=False)
    user_name: Optional[str] = attr.ib(default=None)
    is_sa: bool = attr.ib(default=False)

    def get_rls_user_name(self):
        if self.is_sa:
            return f'@sa:{self.user_id}'
        return self.user_name


def get_user_account_credentials(
        user_oauth_token: str, yc_ts_endpoint: str, as_grpc_channel,
        caches_dir: Optional[str] = None, cache_key: Optional[str] = None,
) -> AccountCredentials:
    cache_file_path = None
    if caches_dir and cache_key:
        cache_file_path = os.path.join(caches_dir, f'iam_token_{cache_key}.caches')
        if os.path.exists(cache_file_path):
            # noinspection PyBroadException
            try:
                with open(cache_file_path, 'r') as f:
                    cached_data = json.load(f)
                    cache_token_expires = datetime.datetime.fromisoformat(cached_data['iam_token_resp']['expiresAt'])
                    expires_in = cache_token_expires - datetime.datetime.now(datetime.timezone.utc)

                    if expires_in >= datetime.timedelta(minutes=30):
                        ret = AccountCredentials(
                            user_id=cached_data['cloud_user_id'],
                            token=cached_data['iam_token_resp']['iamToken'],
                        )
                        return ret
            except Exception:
                pass

    ts_client = DLTSClient.create(endpoint=yc_ts_endpoint)
    iam_token_resp_obj = ts_client.authenticate_by_oauth_resp_sync(user_oauth_token)
    iam_token_resp = dict(
        iamToken=iam_token_resp_obj.iam_token,
        expiresAt=iam_token_resp_obj.expires_at.ToDatetime().isoformat(),
    )
    cloud_user_id = getattr(getattr(iam_token_resp_obj.subject, 'user_account', None), 'id', None)
    # cloud_user_id = get_cloud_user_id(iam_token_resp['iamToken'], as_grpc_channel)

    if caches_dir and cache_key:
        assert cache_file_path
        with open(cache_file_path, 'w') as f:
            json.dump({
                'iam_token_resp': iam_token_resp,
                'cloud_user_id': cloud_user_id,
            }, f)

    return AccountCredentials(
        user_id=cloud_user_id,
        token=iam_token_resp['iamToken'],
    )


def get_service_account_iam_token(
        service_account_id: str, key_id: str, private_key: str,
        yc_ts_endpoint: str,
        caches_dir: Optional[str] = None, cache_key: Optional[str] = None
) -> str:
    cache_file_path = (
        os.path.join(caches_dir, f'iam_token_{cache_key}.caches')
        if caches_dir is not None and cache_key is not None
        else None
    )

    if cache_file_path is not None and os.path.exists(cache_file_path):
        with open(cache_file_path, 'r') as f:
            cached_iam_token_resp = json.load(f)
            cache_token_expires = datetime.datetime.fromisoformat(cached_iam_token_resp['expiresAt'])
            expires_in = cache_token_expires - datetime.datetime.now(datetime.timezone.utc)

        if expires_in >= datetime.timedelta(minutes=30):
            return cached_iam_token_resp['iamToken']

    ts_client = DLTSClient.create(endpoint=yc_ts_endpoint)
    iam_token_resp_obj = ts_client.create_token_resp_sync(
        service_account_id=service_account_id,
        key_id=key_id,
        private_key=private_key,
        expiration=3600,
    )
    iam_token_resp = dict(
        iamToken=iam_token_resp_obj.iam_token,
        expiresAt=iam_token_resp_obj.expires_at.ToDatetime().isoformat(),
    )

    if cache_file_path is not None:
        with open(cache_file_path, 'w') as f:
            json.dump(iam_token_resp, f)

    return iam_token_resp['iamToken']


@attr.s(auto_attribs=True, frozen=True)
class CloudCredentialsConverter:
    yc_ts_endpoint: str
    as_grpc_channel: grpc.Channel
    caches_dir: Optional[str] = None

    def get_user_account_credentials(
            self,
            user_oauth_token: str,
            cache_key: Optional[str] = None,
    ) -> AccountCredentials:
        return get_user_account_credentials(
            user_oauth_token=user_oauth_token,
            yc_ts_endpoint=self.yc_ts_endpoint,
            as_grpc_channel=self.as_grpc_channel,
            caches_dir=self.caches_dir,
            cache_key=cache_key,
        )

    def get_service_account_iam_token(
            self,
            service_account_id: str,
            key_id: str,
            private_key: str,
            cache_key: Optional[str] = None,
    ):
        return get_service_account_iam_token(
            service_account_id=service_account_id,
            key_id=key_id,
            private_key=private_key,
            yc_ts_endpoint=self.yc_ts_endpoint,
            caches_dir=self.caches_dir,
            cache_key=cache_key,
        )

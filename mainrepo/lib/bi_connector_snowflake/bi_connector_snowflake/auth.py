from __future__ import annotations

from datetime import (
    datetime,
    timedelta,
)
from typing import Optional
from urllib import parse

import attr
import requests

from bi_connector_snowflake.core.dto import SnowFlakeConnDTO
from bi_connector_snowflake.core.exc import SnowflakeGetAccessTokenError
from bi_api_commons.aiohttp.aiohttp_client import PredefinedIntervalsRetrier, BIAioHTTPClient, THeaders


@attr.s()
class AccessTokenRequestArgs:
    endpoint: str = attr.ib()
    params: dict[str, str] = attr.ib()
    basic_auth: tuple[str, str] = attr.ib()

    @property
    def auth_as_headers(self) -> THeaders:
        return {
            "Authorization": requests.auth._basic_auth_str(*self.basic_auth),
        }


@attr.s()
class SFAuthProvider:
    account_name: str = attr.ib()
    user_name: str = attr.ib()
    client_id: str = attr.ib()
    client_secret: str = attr.ib()
    refresh_token: str = attr.ib()
    refresh_token_expire_time: Optional[datetime] = attr.ib(default=None)

    _SNOWFLAKE_DOMAIN = "snowflakecomputing.com"
    _NOTIFICATION_DAYS_BEFORE_TOKEN_EXPIRE = 7

    @classmethod
    def from_dto(cls, dto: SnowFlakeConnDTO) -> SFAuthProvider:
        return cls(
            account_name=dto.account_name,
            user_name=dto.user_name,
            client_id=dto.client_id,
            client_secret=dto.client_secret,
            refresh_token=dto.refresh_token,
            refresh_token_expire_time=dto.refresh_token_expire_time,
        )

    def prepare_access_token_request_kwargs(self) -> AccessTokenRequestArgs:
        return AccessTokenRequestArgs(
            endpoint=f"https://{self.account_name}.{self._SNOWFLAKE_DOMAIN}/oauth/token-request",
            params=dict(
                refresh_token=self.refresh_token,
                grant_type="refresh_token",
                # snowflake documentation mentions redirect uri, but works fine as is
                # redirect_uri="http://localhost:8000",
            ),
            basic_auth=(self.client_id, parse.quote(self.client_secret)),
        )

    def _store_access_token(self, access_token: str, ttl: Optional[int] = None) -> None:
        pass

    async def async_get_access_token(self) -> str:
        ra = self.prepare_access_token_request_kwargs()
        async with BIAioHTTPClient(
            base_url=ra.endpoint,
            conn_timeout_sec=1,
            read_timeout_sec=2,
            raise_for_status=False,
            retrier=PredefinedIntervalsRetrier(retry_intervals=(0.2, 0.3, 0.5), retry_methods={"POST"}),
            session=None,
        ) as http_client:
            async with http_client.request(
                method="POST",
                data=ra.params,
                headers=ra.auth_as_headers,
                conn_timeout_sec=1,
            ) as response:
                try:
                    raw = await response.read()

                    if response.status == 200:
                        try:
                            json_data = await response.json()
                            return json_data["access_token"]
                        except Exception:  # noqa
                            pass

                    raise SnowflakeGetAccessTokenError(
                        "Failed to get access token, try to obtain new refresh token",
                        details={
                            "status_code": response.status,
                            "raw_response": repr(raw),
                        },
                    )
                except Exception:
                    raise SnowflakeGetAccessTokenError(
                        "Failed to get access token, try to obtain new refresh token",
                        details={
                            "status_code": response.status,
                            "raw_response": repr(raw),
                        },
                    )

    def should_notify_refresh_token_to_expire_soon(self) -> bool:
        if self.refresh_token_expire_time is None:
            return False

        td = timedelta(days=self._NOTIFICATION_DAYS_BEFORE_TOKEN_EXPIRE)
        return self.refresh_token_expire_time - datetime.now() < td

    def is_refresh_token_expired(self) -> bool:
        if self.refresh_token_expire_time is None:
            return False

        return self.refresh_token_expire_time < datetime.now()

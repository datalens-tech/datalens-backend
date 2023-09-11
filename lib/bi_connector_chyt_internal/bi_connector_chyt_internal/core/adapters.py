from __future__ import annotations

import abc
import re
from typing import TYPE_CHECKING, Optional, Type, ClassVar

import attr

from bi_core.connection_executors.models.db_adapter_data import RawIndexInfo

from bi_connector_chyt.core.adapters import CHYTConnLineConstructor, BaseCHYTAdapter
from bi_connector_chyt_internal.core.utils import get_chyt_user_auth_headers

from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)


if TYPE_CHECKING:
    from bi_connector_chyt_internal.core.target_dto import (
        BaseCHYTInternalConnTargetDTO,
        CHYTInternalConnTargetDTO,
        CHYTUserAuthConnTargetDTO,
    )


class BaseCHYTInternalAdapter(BaseCHYTAdapter, abc.ABC):
    _target_dto: BaseCHYTInternalConnTargetDTO = attr.ib()

    async def _get_yt_table_index_info(
        self,
        table_path: str,
        secret_auth_headers: dict[str, str],  # Strange name to prevent leaking to Sentry
    ) -> Optional[RawIndexInfo]:
        yt_cluster = self._target_dto.yt_cluster
        yt_cluster_name_pattern = re.compile(r"^[a-z][a-z-]{1,16}[a-z]$")
        assert isinstance(yt_cluster, str) and yt_cluster_name_pattern.match(yt_cluster), \
            f"YT cluster name must be string and matches {yt_cluster_name_pattern.pattern!r}, not {yt_cluster!r}"
        assert isinstance(table_path, str) and table_path.startswith("//"), "Incorrect YT table path"

        return await self._fetch_yt_sorting_columns(
            host=f'{yt_cluster}.yt.yandex.net',
            table_path=table_path,
            secret_auth_headers=secret_auth_headers,
        )


class CHYTInternalAdapter(BaseCHYTInternalAdapter):
    conn_type = CONNECTION_TYPE_CH_OVER_YT

    _target_dto: CHYTInternalConnTargetDTO = attr.ib()

    def _get_yt_proxy_auth_headers(self) -> dict[str, str]:
        return {'Authorization': f"OAuth {self._target_dto.password}"}


class CHYTUserAuthConnLineConstructor(CHYTConnLineConstructor):
    def _get_dsn_params(
            self,
            safe_db_symbols: tuple[str, ...] = (),
            db_name: Optional[str] = None,
            standard_auth: Optional[bool] = True,
    ) -> dict:
        return super()._get_dsn_params(safe_db_symbols=safe_db_symbols, db_name=db_name, standard_auth=False)


class CHYTUserAuthAdapter(BaseCHYTInternalAdapter):
    conn_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH

    _target_dto: CHYTUserAuthConnTargetDTO = attr.ib()

    dsn_template = '{dialect}://{host}:{port}/{db_name}'
    conn_line_constructor_type: ClassVar[Type[CHYTUserAuthConnLineConstructor]] = CHYTUserAuthConnLineConstructor

    def _get_yt_proxy_auth_headers(self) -> dict[str, str]:
        return get_chyt_user_auth_headers(
            authorization=self._target_dto.header_authorization,
            cookie=self._target_dto.header_cookie,
            csrf_token=self._target_dto.header_csrf_token,
        )

    def get_connect_args(self) -> dict:
        return {
            **super().get_connect_args(),
            **self._convert_headers_to_dsn_params(self._get_yt_proxy_auth_headers()),
        }

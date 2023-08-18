from __future__ import annotations

from typing import TYPE_CHECKING, Mapping, Dict

from bi_blackbox_client.authenticate import authenticate_async
from bi_utils.sanitize import param_bool

if TYPE_CHECKING:
    from multidict import CIMultiDictProxy


ALERTS_OAUTH_SCOPES = (
    'startrek:read', 'yt:api', 'stat:all', 'yql:api', 'solomon:read', 'grafana:api', 'charts:read',
)


async def check_oauth_token(token: str, userip: str, host: str) -> dict:
    auth_res = await authenticate_async(
        authorization_header=f'OAuth {token}',
        userip=userip,
        host=host,
        scopes=ALERTS_OAUTH_SCOPES,
        force_tvm=True,
    )

    return auth_res


async def check_cookies(cookies: Mapping[str, str], userip: str, host: str) -> dict:
    auth_res = await authenticate_async(
        session_id_cookie=cookies.get('Session_id'),
        sessionid2_cookie=cookies.get('sessionid2'),
        userip=userip,
        host=host,
        force_tvm=True,
    )

    return auth_res


def is_dl_superuser(auth_data: Dict, headers: CIMultiDictProxy[str]) -> bool:
    # TODO: https://st.yandex-team.ru/BI-2412
    _users_whitelist = (
        # https://staff.yandex-team.ru/persons/yandex_infra_data_dep08885_dep00642_dep82187
        'dmifedorov', 'seray', 'thenno', 'asnytin', 'gstatsenko', 'kchupin',
        # https://staff.yandex-team.ru/persons/yandex_infra_tech_interface_monitoring_dep67825
        'resure', 'alaev89', 'marginy', 'apanchuk', 'vrozaev', 'flunt1k', 'inchernobuk', 'shangin',
    )

    if param_bool(headers.get('x-dl-allow-superuser', 'false')) and auth_data.get('username') in _users_whitelist:
        return True

    return False

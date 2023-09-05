"""
Tooling for requesting through gozora.

Currently only used in the gsheets adapter.
"""

from typing import Any, Dict, Optional

import aiohttp

from bi_blackbox_client.tvm import TvmCliSingleton, TvmDestination, get_tvm_headers

GOZORA_PROXY = 'http://go.zora.yandex.net:1080'


class TvmCliSingletonGoZora(TvmCliSingleton):
    destinations = (TvmDestination.GoZora,)  # type: ignore  # TODO: fix


async def prepare_aiohttp_kwargs(request_id: Optional[str] = None) -> Dict[str, Any]:
    tvm_cli = await TvmCliSingletonGoZora.get_tvm_cli()
    tvm_headers = get_tvm_headers(tvm_cli=tvm_cli, destination=TvmDestination.GoZora)
    proxy_headers = {
        **tvm_headers,
    }
    if request_id:
        proxy_headers['X-Ya-Req-Id']: request_id  # type: ignore  # TODO: fix
    proxy_headers['X-Ya-Client-Id'] = 'datalens_backend_internal'
    return dict(
        proxy=GOZORA_PROXY,
        proxy_headers=proxy_headers,
        # returned cert is gozora's: `issuer: C=RU; ST=Moscow; L=Moscow; O=Yandex LLC; CN=Zora Proxy`
        ssl=False,
    )


assert hasattr(aiohttp.TCPConnector, "_loop_supports_start_tls"), \
    "aiohttp.TCPConnector has no _loop_supports_start_tls meth. Path is not working anymore"


class GoZoraTCPConnector(aiohttp.TCPConnector):
    def _loop_supports_start_tls(self) -> bool:
        """
        Workaround due to None transport with `_loop_supports_start_tls` with underlying loop.start_tls
        With this workaround we force connector to use _wrap_create_connection which works ok with GoZora
        https://github.com/aio-libs/aiohttp/blob/v3.8.5/aiohttp/connector.py#L1319C39-L1319C62

        Was written based on aiohttp version 3.8.5
        :return:
        """
        return False

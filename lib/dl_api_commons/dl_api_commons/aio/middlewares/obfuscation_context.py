import aiohttp
from aiohttp.typedefs import (
    Handler,
    Middleware,
)

from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
from dl_obfuscator import (
    OBFUSCATION_ENGINE_KEY,
    setup_request_obfuscation,
    teardown_request_obfuscation,
)


def obfuscation_context_middleware() -> Middleware:
    @aiohttp.web.middleware
    @DLRequestBase.use_dl_request
    async def actual_middleware(
        dl_request: DLRequestBase,
        handler: Handler,
    ) -> aiohttp.web.StreamResponse:
        engine = dl_request.request.app.get(OBFUSCATION_ENGINE_KEY)
        rci = dl_request.last_resort_rci
        secret_keeper = rci.secret_keeper if rci is not None else None

        setup_request_obfuscation(
            engine=engine,
            secret_keeper=secret_keeper,
        )

        try:
            return await handler(dl_request.request)
        finally:
            teardown_request_obfuscation(engine)

    return actual_middleware
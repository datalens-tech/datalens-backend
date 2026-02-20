import aiohttp
from aiohttp.typedefs import (
    Handler,
    Middleware,
)

from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
from dl_obfuscator import (
    OBFUSCATION_BASE_OBFUSCATORS_KEY,
    create_request_engine,
)
from dl_obfuscator.request_context import (
    clear_request_obfuscation_engine,
    set_request_obfuscation_engine,
)


def obfuscation_context_middleware() -> Middleware:
    @aiohttp.web.middleware
    @DLRequestBase.use_dl_request
    async def actual_middleware(
        dl_request: DLRequestBase,
        handler: Handler,
    ) -> aiohttp.web.StreamResponse:
        base_obfuscators = dl_request.request.app.get(OBFUSCATION_BASE_OBFUSCATORS_KEY)

        if base_obfuscators is not None:
            rci = dl_request.last_resort_rci
            secret_keeper = rci.secret_keeper if rci is not None else None

            engine = create_request_engine(
                base_obfuscators=base_obfuscators,
                secret_keeper=secret_keeper,
            )

            set_request_obfuscation_engine(engine)

            if rci is not None:
                dl_request.update_temp_rci(obfuscation_engine=engine)

        try:
            return await handler(dl_request.request)
        finally:
            clear_request_obfuscation_engine()

    return actual_middleware

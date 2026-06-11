import aiohttp
from aiohttp.typedefs import (
    Handler,
    Middleware,
)

from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestBase,
    RCINotSet,
)
from dl_obfuscator import (
    OBFUSCATION_BASE_OBFUSCATORS_KEY,
    create_request_engine,
)
from dl_obfuscator.profiling import (
    clear_log_format_profiling,
    dump_log_format_profiling,
    init_log_format_profiling,
)
from dl_obfuscator.request_context import (
    clear_request_obfuscation_engine,
    set_request_obfuscation_engine,
)


def obfuscation_context_middleware(
    log_format_profiling_enabled: bool = False,
) -> Middleware:
    @aiohttp.web.middleware
    @DLRequestBase.use_dl_request
    async def actual_middleware(
        dl_request: DLRequestBase,
        handler: Handler,
    ) -> aiohttp.web.StreamResponse:
        if log_format_profiling_enabled:
            init_log_format_profiling()

        base_obfuscators = dl_request.request.app.get(OBFUSCATION_BASE_OBFUSCATORS_KEY)

        if base_obfuscators is not None:
            try:
                rci = dl_request.temp_rci
            except RCINotSet:
                rci = None

            secret_keeper = rci.secret_keeper if rci is not None else None

            engine = create_request_engine(
                base_obfuscators=base_obfuscators,
                secret_keeper=secret_keeper,
            )

            set_request_obfuscation_engine(engine)

            if rci is not None:
                dl_request.update_temp_rci(obfuscation_engine=engine)
                rci.populate_secret_keeper()

        try:
            return await handler(dl_request.request)
        finally:
            # clear() is a safety net: dump() is a no-op when call_count == 0 or context is None
            if log_format_profiling_enabled:
                dump_log_format_profiling()
                clear_log_format_profiling()
            clear_request_obfuscation_engine()

    return actual_middleware

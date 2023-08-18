from __future__ import annotations

from .env_var_reader import get_from_env
from .utils import jaeger_service_suffix_env_var_converter


def use_jaeger_tracer() -> bool:
    return get_from_env("DL_USE_JAEGER_TRACER", lambda s: bool(int(s)), default=False)


def jaeger_service_name_env_aware(service_name: str) -> str:
    return service_name + get_from_env(
        "YENV_TYPE",
        jaeger_service_suffix_env_var_converter,
        "",  # Empty suffix if YENV_TYPE is not specified
    )


def do_data_source_indexes_fetch() -> bool:
    return get_from_env("DL_DO_DS_IDX_FETCH", lambda s: bool(int(s)), default=False)

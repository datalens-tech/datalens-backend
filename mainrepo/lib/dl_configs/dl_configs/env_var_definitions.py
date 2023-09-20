from __future__ import annotations

from dl_configs.env_var_reader import get_from_env


def use_jaeger_tracer() -> bool:
    return get_from_env("DL_USE_JAEGER_TRACER", lambda s: bool(int(s)), default=False)


def jaeger_service_name_env_aware(service_name: str) -> str:
    return service_name + get_from_env("DL_JAEGER_TRACER_SERVICE_NAME_SUFFIX", str, "")

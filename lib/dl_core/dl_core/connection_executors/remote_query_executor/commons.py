from __future__ import annotations

from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter


DEFAULT_CHUNK_SIZE: int = 1_000

SUPPORTED_ADAPTER_CLS: set[type[CommonBaseDirectAdapter]] = set()


def register_adapter_class(adapter_cls: type[CommonBaseDirectAdapter]) -> None:
    SUPPORTED_ADAPTER_CLS.add(adapter_cls)

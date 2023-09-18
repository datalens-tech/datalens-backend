from __future__ import annotations

from bi_blackbox_client.tvm import (
    TvmCliSingleton,
    TvmDestination,
    TvmDestinationSet,
)
from dl_core.exc import DatabaseQueryError

SOLOMON_TVM_MAP = {
    "https://solomon.yandex.net": TvmDestination.SolomonProd,
    "https://solomon-prestable.yandex.net": TvmDestination.SolomonPre,
}


class TvmCliSingletonSolomon(TvmCliSingleton):
    destinations: TvmDestinationSet = frozenset(
        (
            TvmDestination.SolomonProd,
            TvmDestination.SolomonPre,
        )
    )  # type: ignore  # TODO: fix


def get_solomon_tvm_destination(url: str) -> TvmDestination:
    try:
        tvm_destination = SOLOMON_TVM_MAP[url]
    except KeyError as err:
        raise DatabaseQueryError(
            message="Unexpected Solomon host",
            db_message=str(err),
            query="",
        )
    return tvm_destination

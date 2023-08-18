from __future__ import annotations

import os
from typing import TYPE_CHECKING, NamedTuple, Optional, Any, Dict

import granular_settings
from granular_settings.utils import is_arcadia

if TYPE_CHECKING:
    from bi_core.tvm import TvmDestination


APP_KEY_SETTINGS = 'bi_alerts_settings'


class Settings(NamedTuple):
    # Charts
    CHARTS_BASE_URL: str
    CHARTS_TOKEN: str
    SQLA_DB_CFG_MASTER: dict
    DATASYNC_SHARD_LIMIT: int
    CHARTS_CHECKER_SEMAPHORE: int
    # Logging
    SENTRY_DSN: Optional[str]
    # Solomon
    SOLOMON_BASE_URL: str
    SOLOMON_TOKEN: str
    SOLOMON_PREFIX: str
    SOLOMON_PROJECT: str
    SOLOMON_CLUSTER: str
    SOLOMON_TVM_ID: TvmDestination
    SOLOMON_FETCHER_TVM_ID: TvmDestination


def from_granular_settings(overrides: Optional[Dict[str, Any]] = None) -> Settings:
    s_dict = {}  # type: ignore  # TODO: fix
    granular_settings.set(
        s_dict,
        path=(
            'bi_alerts/settings'
            if is_arcadia else
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'settings')
        ),
    )

    filtered_settings = {
        s_name: s_dict[s_name]
        for s_name in s_dict
        if s_name in Settings._fields
    }

    if overrides is not None:
        filtered_settings.update(overrides)

    return Settings(**filtered_settings)

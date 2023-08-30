import logging
from typing import List

from bi_configs.settings_loaders.common import SDict

LOGGER = logging.getLogger(__name__)

REMAP = {
    "EXT_QUERY_EXECUTER_SECRET_KEY": "RQE_SECRET_KEY",

    # TODO FIX: BI-1753 remove after sensitive info will leave versioned data of US entry
    "FERNET_KEY": "DL_CRY_KEY_VAL_ID_0",
}

_LEGACY_AND_ACTUAL_KEYS_INTERSECTION = set(REMAP.keys()) & set(REMAP.values())

assert not _LEGACY_AND_ACTUAL_KEYS_INTERSECTION, (
    f"Got non-empty intersection of legacy & actual keys:"
    f" {_LEGACY_AND_ACTUAL_KEYS_INTERSECTION}"
)


def remap_env(s_dict: SDict) -> SDict:
    ret = dict(s_dict)

    legacy_keys_to_remap_candidates = s_dict.keys() & REMAP.keys()
    legacy_keys_to_remap: List[str] = []

    for legacy_key in sorted(legacy_keys_to_remap_candidates):
        actual_key = REMAP[legacy_key]

        if actual_key in s_dict:
            LOGGER.warning(
                "Environment contains both legacy (%r) and actual (%r) keys. Actual key taking precedence.",
                legacy_key, actual_key
            )
            continue

        legacy_keys_to_remap.append(legacy_key)

    for legacy_key in legacy_keys_to_remap:
        actual_key = REMAP[legacy_key]
        LOGGER.info("Remapping environment key: %s -> %s", legacy_key, actual_key)
        ret[actual_key] = ret.pop(legacy_key)

    return ret

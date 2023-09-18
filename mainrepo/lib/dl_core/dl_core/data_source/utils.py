from __future__ import annotations

import itertools
from typing import (
    Any,
    Optional,
)

import xxhash

from dl_constants.enums import CreateDSFrom

_IGNORE_IN_HASH = frozenset(("db_version",))


def get_parameters_hash(source_type: CreateDSFrom, connection_id: Optional[str], **parameters: Any) -> str:
    data = (
        source_type,
        connection_id,
        *itertools.chain.from_iterable(
            (key, value)
            for key, value in sorted(parameters.items())
            if value is not None and key not in _IGNORE_IN_HASH
        ),
    )
    return xxhash.xxh64(str(data), seed=0).hexdigest()

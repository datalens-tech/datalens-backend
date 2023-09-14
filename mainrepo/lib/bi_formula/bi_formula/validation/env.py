from __future__ import annotations

from collections import defaultdict
from typing import (
    Dict,
    Optional,
)

import attr

from bi_formula.collections import NodeValueMap
import bi_formula.core.exc as exc


@attr.s
class ErrInfo:
    is_error: bool = attr.ib(default=False)
    exception: Optional[exc.ValidationError] = attr.ib(default=None)


@attr.s
class ValidationEnvironment:
    cache_dim_bound: NodeValueMap[bool] = attr.ib(factory=NodeValueMap)
    generic_cache_valid: Dict[type, NodeValueMap[ErrInfo]] = attr.ib(factory=lambda: defaultdict(NodeValueMap))

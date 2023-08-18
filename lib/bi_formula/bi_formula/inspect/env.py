from __future__ import annotations

import attr

from bi_formula.core.datatype import DataType
from bi_formula.collections import NodeValueMap


@attr.s
class InspectionEnvironment:
    cache_is_const_expr: NodeValueMap[bool] = attr.ib(factory=NodeValueMap)
    cache_is_unbound_expr: NodeValueMap[bool] = attr.ib(factory=NodeValueMap)
    cache_is_aggregate_expr: NodeValueMap[bool] = attr.ib(factory=NodeValueMap)
    cache_is_window_expr: NodeValueMap[bool] = attr.ib(factory=NodeValueMap)
    cache_is_query_fork_expr: NodeValueMap[bool] = attr.ib(factory=NodeValueMap)
    cache_query_fork_nesting: NodeValueMap[int] = attr.ib(factory=NodeValueMap)
    cache_data_type: NodeValueMap[DataType] = attr.ib(factory=NodeValueMap)

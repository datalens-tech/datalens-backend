from __future__ import annotations

from collections import defaultdict
from typing import (
    Any,
    NamedTuple,
)

import attr

from dl_formula.collections import NodeValueMap
from dl_formula.core.datatype import (
    DataType,
    DataTypeParams,
)
from dl_formula.core.dialect import DialectCombo
import dl_formula.translation.context as ctx_mod  # noqa


class FunctionStatsSignature(NamedTuple):
    name: str
    arg_types: tuple[str, ...]
    dialect: str
    is_window: bool

    def as_dict(self) -> dict[str, Any]:
        return self._asdict()


@attr.s
class TranslationStats:
    # {(<function_name>, (<arg_type_names>), <dialect_name>, <is_window>): <usage_count>}
    function_usage_weights: dict[FunctionStatsSignature, int] = attr.ib(factory=lambda: defaultdict(lambda: 0))
    cache_hits: int = attr.ib(kw_only=True, default=0)

    @classmethod
    def combine(cls, *stat_instances: TranslationStats) -> TranslationStats:
        combined_func_usage_weights: dict[FunctionStatsSignature, int] = defaultdict(lambda: 0)
        combined_cache_hits = 0
        for stat_ins in stat_instances:
            combined_cache_hits += stat_ins.cache_hits
            for func_sign, func_value in stat_ins.function_usage_weights.items():
                combined_func_usage_weights[func_sign] += func_value

        return TranslationStats(
            function_usage_weights=combined_func_usage_weights,
            cache_hits=combined_cache_hits,
        )

    def __add__(self, other: TranslationStats) -> TranslationStats:
        assert isinstance(other, TranslationStats)
        return self.combine(self, other)

    def add_function_hit(self, func_signature: FunctionStatsSignature, value: int = 1) -> None:
        self.function_usage_weights[func_signature] += value

    def add_cache_hit(self, value: int = 1) -> None:
        self.cache_hits += value


@attr.s
class TranslationEnvironment:
    dialect: DialectCombo = attr.ib()  # a combination of dialect bits
    required_scopes: int = attr.ib()

    # TODO: combine these into `field_info = {field_id: {name: ..., type: ..., type_params: ...}, ...}`
    field_names: dict[str, tuple[str, ...]] = attr.ib(factory=dict)
    field_types: dict[str, DataType] = attr.ib(factory=dict)
    field_type_params: dict[str, DataTypeParams] = attr.ib(factory=dict)

    translation_cache: NodeValueMap["ctx_mod.TranslationCtx"] = attr.ib(factory=NodeValueMap)
    replacements: NodeValueMap["ctx_mod.TranslationCtx"] = attr.ib(factory=NodeValueMap)

    translation_stats: TranslationStats = attr.ib(kw_only=True, factory=TranslationStats)

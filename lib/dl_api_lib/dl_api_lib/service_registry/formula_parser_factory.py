from __future__ import annotations

import logging
from typing import (
    Dict,
    Optional,
)

import attr

from dl_formula.parser.base import FormulaParser
from dl_formula.parser.factory import (
    ParserType,
    get_parser,
)


_DEFAULT_PARSER_TYPE = ParserType.antlr_py

LOGGER = logging.getLogger(__name__)


@attr.s
class FormulaParserFactory:
    _default_formula_parser_type: Optional[ParserType] = attr.ib(kw_only=True, default=None)
    _saved_parsers: Dict[ParserType, FormulaParser] = attr.ib(init=False, factory=dict)

    def get_formula_parser(self, parser_type: Optional[ParserType] = None) -> FormulaParser:
        if parser_type is None:
            parser_type = self._default_formula_parser_type or _DEFAULT_PARSER_TYPE
        assert parser_type is not None

        if parser_type not in self._saved_parsers:
            self._saved_parsers[parser_type] = get_parser(parser_type=parser_type)

        return self._saved_parsers[parser_type]

    def log_parser_stats_for_type(self, parser_type: ParserType) -> None:
        parser = self._saved_parsers[parser_type]
        stats = parser.get_usage_stats()
        data = {
            "global_cache_hits": stats.global_cache_hits,
            "global_cache_misses": stats.global_cache_misses,
            "global_cache_maxsize": stats.global_cache_maxsize,
            "global_cache_currsize": stats.global_cache_currsize,
            "call_count": stats.call_count,
            "call_time": stats.call_time,
            "length_counts": stats.length_counts,
            "length_avg_times": stats.length_avg_times,
            "length_median_times": stats.length_median_times,
            "parser_type": parser_type.name,
        }
        LOGGER.info(
            f"Formula parser global statistics ({parser_type.name})", extra=dict(function_parser_statistics=data)
        )

    def log_parser_stats_for_all_used_parsers(self) -> None:
        for parser_type in self._saved_parsers:
            self.log_parser_stats_for_type(parser_type=parser_type)

    def close(self) -> None:
        self.log_parser_stats_for_all_used_parsers()

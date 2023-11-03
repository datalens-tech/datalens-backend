"""
See: dl_core/maintenance/README.md
+
crawler.print_stats()  # chars=False)

"""

from __future__ import annotations

from collections import defaultdict
from enum import (
    Enum,
    auto,
    unique,
)
import json
import statistics
from typing import (
    Any,
    AsyncIterable,
    Generic,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import attr

from dl_core.us_entry import (
    USEntry,
    USMigrationEntry,
)
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_maintenance.core.us_crawler_base import USEntryCrawler


DEFAULT_QUANTILE_SETTINGS = (1000, [100, 750, 900, 950, 999])


@unique
class Metric(Enum):
    # Field
    ds_cnt_field_all = auto()
    ds_cnt_field_direct = auto()
    ds_cnt_field_formula = auto()
    ds_cnt_field_parameter = auto()
    ds_len_field_title = auto()
    ds_len_field_formula = auto()
    ds_len_field_formula_line = auto()
    ds_cnt_field_formula_lines = auto()
    ds_len_field_source = auto()
    ds_len_field_guid = auto()
    ds_chars_field_guid = auto()
    ds_chars_field_title = auto()
    ds_chars_field_formula = auto()
    ds_chars_field_source = auto()
    # Source
    ds_cnt_source = auto()
    ds_cnt_source_columns = auto()
    ds_len_source_id = auto()
    ds_len_source_db_name = auto()
    ds_chars_source_db_name = auto()
    ds_len_source_schema_name = auto()
    ds_chars_source_schema_name = auto()
    ds_len_source_table_name = auto()
    ds_chars_source_table_name = auto()
    ds_len_source_table_names = auto()
    ds_chars_source_table_names = auto()
    ds_cnt_source_table_names_lines = auto()
    ds_len_source_table_names_line = auto()
    ds_len_source_subsql = auto()
    ds_chars_source_subsql = auto()
    ds_len_source_subsql_line = auto()
    ds_cnt_source_subsql_lines = auto()
    ds_len_source_column_name = auto()
    ds_len_source_column_title = auto()
    ds_chars_source_column_name = auto()
    ds_chars_source_column_title = auto()
    # Avatar
    ds_cnt_avatar = auto()
    ds_len_avatar_id = auto()
    ds_len_avatar_title = auto()
    ds_chars_avatar_title = auto()
    # Relation
    ds_cnt_relation = auto()
    ds_cnt_relation_condition = auto()
    ds_len_relation_id = auto()
    # Obligatory filter
    ds_cnt_ob_filter = auto()
    ds_cnt_ob_filter_conditions = auto()
    ds_cnt_ob_filter_args = auto()
    ds_len_ob_filter_id = auto()
    ds_len_ob_filter_arg = auto()


M = Metric  # for convenience

_RAW_VALUE_TV = TypeVar("_RAW_VALUE_TV")


class RawValueCollector(Generic[_RAW_VALUE_TV]):
    def add_value(self, value: _RAW_VALUE_TV) -> None:
        raise NotImplementedError

    def get_aggregate(self, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError


@attr.s
class RawIntegerCollector(RawValueCollector[int]):
    _values: list[int] = attr.ib(init=False, factory=list)

    def add_value(self, value: int) -> None:
        self._values.append(value)

    def _get_quantiles(
        self,
        parts: int,
        return_values: Sequence[int],
    ) -> dict[str, Union[int, float]]:
        results = statistics.quantiles(self._values, n=parts, method="inclusive")
        return {f"q{parts}_{ret_idx}": results[ret_idx - 1] for ret_idx in return_values}

    def get_aggregate(
        self,
        quantile_settings: tuple[int, Sequence[int]] = DEFAULT_QUANTILE_SETTINGS,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return dict(
            min=min(self._values),
            max=max(self._values),
            mean=statistics.mean(self._values),
            median=statistics.median(self._values),
            **self._get_quantiles(
                parts=quantile_settings[0],
                return_values=quantile_settings[1],
            ),
        )


@attr.s
class RawCharCollector(RawValueCollector[str]):
    _values: set[str] = attr.ib(init=False, factory=set)

    def add_value(self, value: str) -> None:
        self._values |= set(value)

    def get_aggregate(self, **kwargs: Any) -> dict[str, Any]:
        return dict(chars="".join(sorted(self._values)))


@attr.s
class StatisticsCollector:
    _int_collectors: dict[Metric, RawIntegerCollector] = attr.ib(
        init=False, factory=lambda: defaultdict(RawIntegerCollector)
    )
    _char_collectors: dict[Metric, RawCharCollector] = attr.ib(
        init=False, factory=lambda: defaultdict(RawCharCollector)
    )

    def int_collector(self, metric: Metric) -> RawIntegerCollector:
        return self._int_collectors[metric]

    def char_collector(self, metric: Metric) -> RawCharCollector:
        return self._char_collectors[metric]

    def get_jsonable_dict(
        self,
        quantile_settings: tuple[int, Sequence[int]],
        chars: bool = True,
    ) -> dict[str, dict[str, Any]]:
        result = {
            key.name: coll.get_aggregate(quantile_settings=quantile_settings)
            for key, coll in self._int_collectors.items()
        }
        if chars:
            result.update({key.name: coll.get_aggregate() for key, coll in self._char_collectors.items()})

        return result


@attr.s
class DatasetMetricCollector(USEntryCrawler):
    ENTRY_TYPE = USMigrationEntry

    _stats_collector: StatisticsCollector = attr.ib(init=False, factory=StatisticsCollector)
    _quantile_settings: tuple[int, Sequence[int]] = attr.ib(kw_only=True, default=DEFAULT_QUANTILE_SETTINGS)

    def get_raw_entry_iterator(self, crawl_all_tenants: bool = True) -> AsyncIterable[dict[str, Any]]:
        return self.usm.get_raw_collection(entry_scope="dataset", all_tenants=crawl_all_tenants)

    async def process_entry_get_save_flag(
        self, entry: USEntry, logging_extra: dict[str, Any], usm: Optional[AsyncUSManager] = None
    ) -> tuple[bool, str]:
        data = entry.data
        self._stats_collector.int_collector(M.ds_cnt_field_all).add_value(len(data["result_schema"]))
        self._stats_collector.int_collector(M.ds_cnt_field_formula).add_value(
            len([field for field in data["result_schema"] if field["calc_mode"] == "formula"])
        )
        self._stats_collector.int_collector(M.ds_cnt_field_direct).add_value(
            len([field for field in data["result_schema"] if field["calc_mode"] == "direct"])
        )
        self._stats_collector.int_collector(M.ds_cnt_field_parameter).add_value(
            len([field for field in data["result_schema"] if field["calc_mode"] == "parameter"])
        )
        for field in data["result_schema"]:
            self._stats_collector.int_collector(M.ds_len_field_guid).add_value(len(field["guid"]))
            self._stats_collector.int_collector(M.ds_len_field_title).add_value(len(field["title"]))
            self._stats_collector.char_collector(M.ds_chars_field_guid).add_value(field["guid"])
            self._stats_collector.char_collector(M.ds_chars_field_title).add_value(field["title"])
            if field["calc_mode"] == "formula":
                formula = field["formula"]
                self._stats_collector.int_collector(M.ds_len_field_formula).add_value(len(formula))
                self._stats_collector.char_collector(M.ds_chars_field_formula).add_value(formula)
                formula_lines = formula.split("\n")
                self._stats_collector.int_collector(M.ds_cnt_field_formula_lines).add_value(len(formula_lines))
                for f_line in formula_lines:
                    self._stats_collector.int_collector(M.ds_len_field_formula_line).add_value(len(f_line))
            elif field["calc_mode"] == "direct":
                self._stats_collector.int_collector(M.ds_len_field_source).add_value(len(field["source"]))
                self._stats_collector.char_collector(M.ds_chars_field_source).add_value(field["source"])

        self._stats_collector.int_collector(M.ds_cnt_source).add_value(len(data.get("source_collections", [])))
        for dsrc_coll in data.get("source_collections", []):
            self._stats_collector.int_collector(M.ds_len_source_id).add_value(len(dsrc_coll["id"]))
            origin = dsrc_coll.get("origin")
            if origin is not None:
                origin = dict(origin, **origin.get("parameters", {}))
                if origin.get("db_name"):
                    self._stats_collector.int_collector(M.ds_len_source_db_name).add_value(len(origin["db_name"]))
                    self._stats_collector.char_collector(M.ds_chars_source_db_name).add_value(origin["db_name"])
                if origin.get("schema_name"):
                    self._stats_collector.int_collector(M.ds_len_source_schema_name).add_value(
                        len(origin["schema_name"])
                    )
                    self._stats_collector.char_collector(M.ds_chars_source_schema_name).add_value(origin["schema_name"])
                if origin.get("table_name"):
                    self._stats_collector.int_collector(M.ds_len_source_table_name).add_value(len(origin["table_name"]))
                    self._stats_collector.char_collector(M.ds_chars_source_table_name).add_value(origin["table_name"])
                if origin.get("table_names"):
                    table_names = origin["table_names"]
                    self._stats_collector.int_collector(M.ds_len_source_table_names).add_value(len(table_names))
                    self._stats_collector.char_collector(M.ds_chars_source_table_names).add_value(table_names)
                    table_names_lines = table_names.split("\n")
                    self._stats_collector.int_collector(M.ds_cnt_source_table_names_lines).add_value(
                        len(table_names_lines)
                    )
                    for table_names_line in table_names_lines:
                        self._stats_collector.int_collector(M.ds_len_source_table_names_line).add_value(
                            len(table_names_line)
                        )
                if origin.get("subsql"):
                    subsql = origin["subsql"]
                    self._stats_collector.int_collector(M.ds_len_source_subsql).add_value(len(subsql))
                    self._stats_collector.char_collector(M.ds_chars_source_subsql).add_value(subsql)
                    subsql_lines = subsql.split("\n")
                    self._stats_collector.int_collector(M.ds_cnt_source_subsql_lines).add_value(len(subsql_lines))
                    for sub_line in subsql_lines:
                        self._stats_collector.int_collector(M.ds_len_source_subsql_line).add_value(len(sub_line))
                if origin.get("raw_schema"):
                    self._stats_collector.int_collector(M.ds_cnt_source_columns).add_value(len(origin["raw_schema"]))
                    for column in origin["raw_schema"]:
                        self._stats_collector.int_collector(M.ds_len_source_column_name).add_value(len(column["name"]))
                        self._stats_collector.int_collector(M.ds_len_source_column_title).add_value(
                            len(column["title"])
                        )
                        self._stats_collector.char_collector(M.ds_chars_source_column_name).add_value(column["name"])
                        title = column.get("title")
                        if title is not None:
                            self._stats_collector.char_collector(M.ds_chars_source_column_title).add_value(title)

        self._stats_collector.int_collector(M.ds_cnt_avatar).add_value(len(data.get("source_avatars", [])))
        for avatar in data.get("source_avatars", []):
            self._stats_collector.int_collector(M.ds_len_avatar_id).add_value(len(avatar["id"]))
            if avatar["title"]:
                self._stats_collector.int_collector(M.ds_len_avatar_title).add_value(len(avatar["title"]))
                self._stats_collector.char_collector(M.ds_chars_avatar_title).add_value(avatar["title"])

        self._stats_collector.int_collector(M.ds_cnt_relation).add_value(len(data.get("avatar_relations", [])))
        for rel in data.get("avatar_relations", []):
            self._stats_collector.int_collector(M.ds_len_relation_id).add_value(len(rel["id"]))
            self._stats_collector.int_collector(M.ds_cnt_relation_condition).add_value(len(rel["conditions"]))

        self._stats_collector.int_collector(M.ds_cnt_ob_filter).add_value(len(data.get("obligatory_filters", [])))
        for obf in data.get("obligatory_filters", []):
            self._stats_collector.int_collector(M.ds_len_ob_filter_id).add_value(len(obf["id"]))
            self._stats_collector.int_collector(M.ds_cnt_ob_filter_conditions).add_value(len(obf["default_filters"]))
            for obf_cond in obf["default_filters"]:
                self._stats_collector.int_collector(M.ds_cnt_ob_filter_args).add_value(len(obf_cond["values"]))
                for obf_cond_value in obf_cond["values"]:
                    self._stats_collector.int_collector(M.ds_len_ob_filter_arg).add_value(len(obf_cond_value))

        return False, "..."

    def print_stats(self, chars: bool = True) -> None:
        print(
            json.dumps(
                self._stats_collector.get_jsonable_dict(
                    quantile_settings=self._quantile_settings,
                    chars=chars,
                ),
                sort_keys=True,
                indent=4,
            )
        )

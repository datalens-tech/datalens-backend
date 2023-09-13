from __future__ import annotations

import logging
from collections import defaultdict
from typing import Callable, Optional

import attr

from bi_formula.core.dialect import DialectCombo
from bi_formula.core.message_ctx import FormulaErrorCtx
from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.translation.env import TranslationStats

from bi_query_processing.enums import ExecutionLevel
from bi_query_processing.compilation.primitives import (
    FromObject, AvatarFromObject, SubqueryFromObject,
    CompiledQuery, CompiledMultiQueryBase,
)
from bi_query_processing.translation.primitives import (
    TranslatedMultiQueryBase, TranslatedMultiQuery, TranslatedFlatQuery,
)
from bi_query_processing.translation.flat_translator import FlatQueryTranslator
from bi_query_processing.column_registry import ColumnRegistry
from bi_query_processing.translation.error_collector import FormulaErrorCollector


LOGGER = logging.getLogger(__name__)


@attr.s
class MultiLevelQueryTranslator:
    _source_db_columns: ColumnRegistry = attr.ib(kw_only=True)
    _inspect_env: InspectionEnvironment = attr.ib(kw_only=True)
    _function_scopes: int = attr.ib(kw_only=True)
    _verbose_logging: bool = attr.ib(kw_only=True, default=False)  # noqa
    _avatar_alias_mapper: Callable[[str], str] = attr.ib(kw_only=True, default=lambda s: s)  # noqa
    _dialect: DialectCombo = attr.ib(kw_only=True)
    _compeng_dialect: Optional[DialectCombo] = attr.ib(kw_only=True)

    # Attributes for source_db
    _collect_stats: bool = attr.ib(kw_only=True, default=False)

    def make_source_db_flat_translator(self, columns: ColumnRegistry) -> FlatQueryTranslator:
        return FlatQueryTranslator(
            columns=columns,
            inspect_env=self._inspect_env,
            function_scopes=self._function_scopes,
            dialect=self._dialect,
            avatar_alias_mapper=self._avatar_alias_mapper,
            collect_stats=self._collect_stats,
        )

    def make_compeng_flat_translator(self, columns: ColumnRegistry) -> FlatQueryTranslator:
        assert self._compeng_dialect is not None
        return FlatQueryTranslator(
            columns=columns,
            inspect_env=self._inspect_env,
            function_scopes=self._function_scopes,
            avatar_alias_mapper=self._avatar_alias_mapper,
            collect_stats=self._collect_stats,
            dialect=self._compeng_dialect,
        )

    def _log_info(self, *args, **kwargs) -> None:  # type: ignore  # TODO: fix
        if self._verbose_logging:
            LOGGER.info(*args, **kwargs)

    def _log_collected_stats(
            self, stats_lists: dict[ExecutionLevel, list[TranslationStats]],
    ) -> None:
        """
        Log stats collected by all translators during translation
        """

        for level_type in sorted(stats_lists, key=lambda lt: lt.name):
            combined_stats = TranslationStats.combine(*stats_lists[level_type])
            data = {
                'functions': [
                    dict(signature.as_dict(), weight=weight)
                    for signature, weight in combined_stats.function_usage_weights.items()
                ],
                'level_type': level_type.name,
                'cache_hits': combined_stats,
            }
            LOGGER.info(
                f'Function translation statistics for {level_type.name}',
                extra=dict(function_translation_statistics=data)
            )

    def _log_query_complexity_stats(self, compiled_multi_query: CompiledMultiQueryBase) -> None:
        LOGGER.info(
            'Query structural info',
            extra=dict(query_struct_info=dict(
                complexity=compiled_multi_query.get_complexity(),
                subquery_count=compiled_multi_query.query_count(),
            ))
        )

    def _get_flat_translator_for_level_type(
            self, level_type: ExecutionLevel, columns: ColumnRegistry,
    ) -> FlatQueryTranslator:
        flat_trans: FlatQueryTranslator
        if level_type == ExecutionLevel.source_db:
            flat_trans = self.make_source_db_flat_translator(columns=columns)
        elif level_type == ExecutionLevel.compeng:
            flat_trans = self.make_compeng_flat_translator(columns=columns)
        else:
            raise ValueError(level_type)
        return flat_trans

    def _make_column_reg_for_from_obj(
            self, from_obj: FromObject, translated_queries_by_id: dict[str, TranslatedFlatQuery],
    ) -> ColumnRegistry:
        """Generate pair <source_id, avatar_id> for using a FromObject in a ColumnRegistry"""
        if isinstance(from_obj, AvatarFromObject):
            return self._source_db_columns
        if isinstance(from_obj, SubqueryFromObject):
            trans_from_query = translated_queries_by_id[from_obj.id]
            column_reg = ColumnRegistry()
            avatar_id = from_obj.query_id
            source_id = from_obj.query_id
            column_reg.register_avatar(avatar_id=avatar_id, source_id=source_id)
            for column in trans_from_query.column_list:
                column_reg.register_column(db_column=column, avatar_id=avatar_id, column_id=column.name)
            return column_reg
        raise TypeError(type(from_obj))

    def translate_multi_query(
            self,
            compiled_multi_query: CompiledMultiQueryBase,
            collect_errors: bool = False,
    ) -> TranslatedMultiQueryBase:

        def _translate_subquery(compiled_flat_subquery: CompiledQuery) -> None:
            from_ids = set(compiled_flat_subquery.joined_from.iter_ids())
            self._log_info(
                f'Translating flat query with id {compiled_flat_subquery.id}, '
                f'using from-objects {from_ids}'
            )
            joined_from_column_reg = ColumnRegistry()
            for from_obj in compiled_flat_subquery.joined_from.froms:
                from_column_reg = self._make_column_reg_for_from_obj(
                    from_obj=from_obj, translated_queries_by_id=translated_queries_by_id,
                )
                joined_from_column_reg += from_column_reg

            flat_trans = self._get_flat_translator_for_level_type(
                level_type=compiled_flat_subquery.level_type, columns=joined_from_column_reg,
            )
            trans_flat_query = flat_trans.translate_flat_query(
                compiled_flat_query=compiled_flat_subquery,
                collect_errors=collect_errors,
            )
            translated_queries_by_id[trans_flat_query.id] = trans_flat_query
            stats_lists_by_level_type[compiled_flat_subquery.level_type].append(flat_trans.get_collected_stats())

        def _recursively_translate_query(compiled_flat_query: CompiledQuery) -> None:
            required_subquery_ids = compiled_flat_query.joined_from.iter_ids()
            for req_id in required_subquery_ids:
                if req_id in base_from_ids:
                    continue  # Nothing to do for base FROMs (source avatars)
                if req_id not in translated_queries_by_id:
                    # Subquery is not yet translated, so go ahead and do it
                    compiled_flat_subquery = compiled_multi_query.get_query_by_id(req_id)
                    _recursively_translate_query(compiled_flat_subquery)
                    assert req_id in translated_queries_by_id

            _translate_subquery(compiled_flat_query)

        base_from_ids = set(compiled_multi_query.get_base_froms().keys())
        translated_queries_by_id: dict[str, TranslatedFlatQuery] = {}
        stats_lists_by_level_type: dict[ExecutionLevel, list[TranslationStats]] = defaultdict(list)
        for compiled_top_query in compiled_multi_query.get_top_queries():
            _recursively_translate_query(compiled_top_query)

        if self._collect_stats:
            self._log_collected_stats(stats_lists_by_level_type)
            self._log_query_complexity_stats(compiled_multi_query)

        return TranslatedMultiQuery(queries=list(translated_queries_by_id.values()))

    def collect_errors(
            self,
            compiled_multi_query: CompiledMultiQueryBase,
            feature_errors: bool = True
    ) -> list[FormulaErrorCtx]:
        with FormulaErrorCollector() as collector:
            self.translate_multi_query(compiled_multi_query=compiled_multi_query, collect_errors=True)
        return collector.get_errors(feature_errors=feature_errors)

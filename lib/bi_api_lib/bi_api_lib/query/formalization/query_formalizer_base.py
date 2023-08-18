from __future__ import annotations

import abc
import logging
from typing import AbstractSet, Collection, List, Optional, Sequence, Tuple

import attr

from bi_core.constants import DataAPILimits
from bi_core.components.ids import AvatarId, FieldId

from bi_query_processing.compilation.query_meta import QueryMetaInfo

from bi_query_processing.legend.block_legend import (
    BlockSpec,
)
from bi_query_processing.compilation.specs import (
    FilterFieldSpec, OrderByFieldSpec, QuerySpec, ParameterValueSpec,
    FilterSourceColumnSpec, RelationSpec, SelectFieldSpec, GroupByFieldSpec,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class QuerySpecFormalizerBase(abc.ABC):
    """
    Normalizes lists of field IDs/titles or more complex specs (for filters and ORDER BY)
    for different parts of the future query and combines them into a single ``QuerySpec`` object.
    """

    _verbose_logging: bool = attr.ib(kw_only=True, default=False)

    def _log_info(self, *args, **kwargs) -> None:  # type: ignore  # TODO: fix
        if self._verbose_logging:
            LOGGER.info(*args, **kwargs)

    @abc.abstractmethod
    def make_phantom_select_ids(
            self, block_spec: BlockSpec,
            order_by_specs: Sequence[OrderByFieldSpec],
    ) -> List[FieldId]:
        raise NotImplementedError

    @abc.abstractmethod
    def make_select_specs(
            self, block_spec: BlockSpec,
            phantom_select_ids: List[FieldId],
    ) -> List[SelectFieldSpec]:
        raise NotImplementedError

    @abc.abstractmethod
    def make_group_by_specs(
            self, block_spec: BlockSpec,
            select_specs: Sequence[SelectFieldSpec],
    ) -> List[GroupByFieldSpec]:
        raise NotImplementedError

    @abc.abstractmethod
    def make_filter_specs(
            self, block_spec: BlockSpec,
    ) -> List[FilterFieldSpec]:
        raise NotImplementedError

    @abc.abstractmethod
    def make_order_by_specs(
            self, block_spec: BlockSpec,
    ) -> List[OrderByFieldSpec]:
        raise NotImplementedError

    @abc.abstractmethod
    def make_source_column_filter_specs(self) -> List[FilterSourceColumnSpec]:
        raise NotImplementedError

    @abc.abstractmethod
    def make_relation_and_avatar_specs(
            self,
            used_field_ids: Collection[FieldId],
    ) -> Tuple[List[RelationSpec], AbstractSet[AvatarId], Optional[AvatarId]]:
        raise NotImplementedError

    @abc.abstractmethod
    def make_parameter_value_specs(
            self, block_spec: BlockSpec,
    ) -> List[ParameterValueSpec]:
        raise NotImplementedError

    def make_limit_offset(self, block_spec: BlockSpec) -> Tuple[Optional[int], Optional[int]]:
        return block_spec.limit, block_spec.offset

    def make_query_meta(
            self, block_spec: BlockSpec,
            phantom_select_ids: List[FieldId],
            select_specs: List[SelectFieldSpec],
            root_avatar_id: Optional[AvatarId],
    ) -> QueryMetaInfo:
        row_count_hard_limit = block_spec.row_count_hard_limit
        if row_count_hard_limit is None:
            row_count_hard_limit = DataAPILimits.DEFAULT_SOURCE_DB_LIMIT
        assert row_count_hard_limit is not None
        row_count_hard_limit = min(row_count_hard_limit, DataAPILimits.DEFAULT_SOURCE_DB_LIMIT)

        query_meta = QueryMetaInfo(
            query_type=block_spec.query_type,
            phantom_select_ids=phantom_select_ids,
            field_order=[
                (idx, field_spec.field_id)
                for idx, field_spec in enumerate(select_specs)
                if field_spec.field_id not in phantom_select_ids
            ],
            row_count_hard_limit=row_count_hard_limit,
            empty_query_mode=block_spec.empty_query_mode,
        )
        return query_meta

    def make_query_spec(self, block_spec: BlockSpec) -> QuerySpec:

        order_by_specs = self.make_order_by_specs(block_spec=block_spec)
        phantom_select_ids = self.make_phantom_select_ids(
            block_spec=block_spec,
            order_by_specs=order_by_specs)
        select_specs = self.make_select_specs(
            block_spec=block_spec,
            phantom_select_ids=phantom_select_ids)
        group_by_specs = self.make_group_by_specs(
            block_spec=block_spec,
            select_specs=select_specs)
        filter_specs = self.make_filter_specs(block_spec=block_spec)
        used_field_ids = {spec.field_id for spec in select_specs} | {
            fs.field_id for fs in filter_specs
        } | {obs.field_id for obs in order_by_specs}
        relation_specs, required_avatar_ids, root_avatar_id = self.make_relation_and_avatar_specs(
            used_field_ids=used_field_ids)
        source_column_filter_specs = self.make_source_column_filter_specs()
        parameter_value_specs = self.make_parameter_value_specs(block_spec=block_spec)
        limit, offset = self.make_limit_offset(block_spec=block_spec)

        query_meta = self.make_query_meta(
            block_spec=block_spec,
            phantom_select_ids=phantom_select_ids,
            select_specs=select_specs,
            root_avatar_id=root_avatar_id,
        )

        return QuerySpec(
            select_specs=select_specs,
            group_by_specs=group_by_specs,
            filter_specs=filter_specs,
            source_column_filter_specs=source_column_filter_specs,
            order_by_specs=order_by_specs,
            relation_specs=relation_specs,
            parameter_value_specs=parameter_value_specs,
            limit=limit,
            offset=offset,
            root_avatar_id=root_avatar_id,
            required_avatar_ids=required_avatar_ids,
            meta=query_meta,
        )

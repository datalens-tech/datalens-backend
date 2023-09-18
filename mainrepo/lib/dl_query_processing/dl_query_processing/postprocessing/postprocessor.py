from __future__ import annotations

import abc
import logging
import re
from typing import (
    Any,
    ClassVar,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
)

import attr

from dl_app_tools.profiling_base import GenericProfiler
from dl_constants.enums import LegendItemType
from dl_query_processing.enums import QueryType
from dl_query_processing.execution.primitives import ExecutedQuery
from dl_query_processing.legend.block_legend import BlockSpec
from dl_query_processing.legend.field_legend import TemplateRoleSpec
from dl_query_processing.postprocessing.postprocessors.all import postprocess_data
from dl_query_processing.postprocessing.primitives import (
    PostprocessedQuery,
    PostprocessedQueryMetaInfo,
)

LOGGER = logging.getLogger(__name__)


# FIXME: Make output typing more specific - should comply with `PostprocessedValue`
#  (especially in MARKUP postprocessor).


@attr.s(frozen=True)
class ValueRestorerBase(abc.ABC):
    """
    Base class for value restorers.
    Main purpose is to get the value of a specific legend item
    from the given raw data row.
    """

    @abc.abstractmethod
    def restore_value(self, raw_row: Sequence[Any]) -> Any:
        raise NotImplementedError


@attr.s(frozen=True)
class IndexValueRestorer(ValueRestorerBase):
    """Restores value simply by retrieving it at the given index"""

    row_idx: int = attr.ib(kw_only=True)

    def restore_value(self, raw_row: Sequence[Any]) -> Any:
        return raw_row[self.row_idx]


@attr.s(frozen=True)
class TemplateValueRestorer(ValueRestorerBase):
    """Restores value simply by applying values to a string template"""

    template_re: ClassVar[re.Pattern] = re.compile(r"\{\{(?P<field>[^{}]+)\}\}")

    template: Optional[str] = attr.ib(kw_only=True)
    idx_by_field_id: Dict[str, int] = attr.ib(kw_only=True)

    def restore_value(self, raw_row: Sequence[Any]) -> Any:
        if self.template is None:
            return None

        def sub_value(field_match: re.Match) -> str:
            field_id = field_match.group("field").strip()
            idx = self.idx_by_field_id[field_id]
            return raw_row[idx]

        return self.template_re.sub(sub_value, self.template)


@attr.s
class DataPostprocessor:
    profiler_prefix: str = attr.ib(kw_only=True)

    def _make_value_restorers(
        self,
        executed_query: ExecutedQuery,
        block_spec: BlockSpec,
    ) -> List[ValueRestorerBase]:
        # Mapping of which field corresponds to which index in the data stream
        field_order = executed_query.meta.field_order
        assert field_order is not None
        idx_by_field_id = {field_id: idx for idx, field_id in field_order}
        result: List[ValueRestorerBase] = []
        for legend_item_id in block_spec.legend_item_ids:
            legend_item = block_spec.legend.get_item(legend_item_id)
            restorer: ValueRestorerBase
            if legend_item.obj.item_type == LegendItemType.placeholder:
                role_spec = legend_item.role_spec
                assert isinstance(role_spec, TemplateRoleSpec)
                restorer = TemplateValueRestorer(template=role_spec.template, idx_by_field_id=idx_by_field_id)
            else:
                restorer = IndexValueRestorer(row_idx=idx_by_field_id[legend_item.id])

            result.append(restorer)

        return result

    def get_postprocessed_data(
        self,
        executed_query: ExecutedQuery,
        block_spec: BlockSpec,
    ) -> PostprocessedQuery:
        query_meta = executed_query.meta
        data: Iterable[Sequence[Any]] = executed_query.rows

        ordered_value_restorers = self._make_value_restorers(executed_query=executed_query, block_spec=block_spec)

        def restore_order(data: Iterable[Sequence[Any]]) -> Generator[Tuple[Any, ...], None, None]:
            for row in data:
                yield tuple(restorer.restore_value(row) for restorer in ordered_value_restorers)

        with GenericProfiler(f"{self.profiler_prefix}-response-prepare"):
            result_fields_types = query_meta.detailed_types
            assert result_fields_types is not None

            postprocessed_data = postprocess_data(data, result_fields_types)

            # Filter out phantom fields, restore deduplicated columns, restore column order
            if executed_query.meta.query_type != QueryType.value_range:
                # FIXME: Dirty hack -> remove this if
                # If this is a value range query, then
                # a) restoration isn't necessary;
                # b) it doesn't work because two columns have the same field_id, but different wrappers (min and max)
                postprocessed_data = tuple(restore_order(postprocessed_data))

        LOGGER.info(
            f"Returning dataset data: {len(postprocessed_data)} rows " f"with {len(result_fields_types)} columns",
            extra=dict(
                fetched_data_statistics=dict(
                    row_count=len(postprocessed_data),
                    column_count=len(result_fields_types),
                ),
            ),
        )

        postprocessed_query = PostprocessedQuery(
            postprocessed_data=postprocessed_data, meta=PostprocessedQueryMetaInfo.from_exec_meta(exec_meta=query_meta)
        )

        return postprocessed_query

from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
)

from dl_api_commons.reporting.models import NotificationReportingRecord
from dl_api_commons.reporting.registry import ReportingRegistry
from dl_api_lib.api_common.data_types import bi_to_yql
from dl_api_lib.schemas.data import (
    DataApiV2ResponseSchema,
    DatasetFieldsResponseSchema,
    NotificationSchema,
)
from dl_api_lib.schemas.legend import LegendSchema
from dl_api_lib.schemas.pivot import (
    PivotHeaderInfoSchema,
    PivotItemSchema,
)
from dl_constants.enums import ManagedBy
from dl_core.us_dataset import Dataset
from dl_query_processing.enums import QueryType
from dl_query_processing.legend.field_legend import LegendItem
from dl_query_processing.merging.primitives import MergedQueryDataStream
from dl_utils.utils import enum_not_none


if TYPE_CHECKING:
    from dl_constants.types import TJSONLike
    from dl_pivot.primitives import DataCellVector
    from dl_pivot.table import PivotTable


LOGGER = logging.getLogger(__name__)


class DataRequestResponseSerializerV2Mixin:
    # TODO: Split this into subclasses

    @classmethod
    def make_data_response_v2_block_meta(cls, merged_stream: MergedQueryDataStream) -> List[Dict[str, Any]]:
        return [
            {
                "block_id": block.block_id,
                "query": block.debug_query,
            }
            for block in merged_stream.meta.blocks
        ]


class DataRequestResponseSerializer(DataRequestResponseSerializerV2Mixin):
    """Serializes data request responses"""

    _SHOW_QUERY_FOR_QUERY_TYPES = frozenset(
        (
            QueryType.result,
            QueryType.pivot,
        )
    )

    @staticmethod
    def get_yql_schema(legend_items: List[LegendItem]) -> list:
        """
        Create YQL schema for given list of fields (`self._query_spec.select_specs`).
        Is needed in the API response to be used by frontend to render results
        """
        return [
            "ListType",
            [
                "StructType",
                [
                    [
                        item.title,  # No longer used by frontend
                        ["OptionalType", ["DataType", bi_to_yql(enum_not_none(item.data_type))]],
                    ]
                    for item in legend_items  # FIXME
                ],
            ],
        ]

    @classmethod
    def make_data_response_v1(
        cls,
        merged_stream: MergedQueryDataStream,
        totals: Optional[Sequence] = None,
        totals_query: Optional[str] = None,
        data_export_forbidden: Optional[bool] = None,
        fields_data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        legend_item_ids = merged_stream.legend_item_ids
        assert legend_item_ids is not None  # in v1 there is only one stream
        query_type = merged_stream.meta.blocks[0].query_type
        if query_type == QueryType.value_range:
            legend_item_ids = [legend_item_ids[0]]
        legend_items = [merged_stream.legend.get_item(liid) for liid in legend_item_ids]
        data: Dict[str, Any] = {
            "result": {
                "data": {
                    "Data": [row.data for row in merged_stream.rows],
                    "Type": cls.get_yql_schema(legend_items),
                },
            },
        }

        if merged_stream.meta.blocks[0].debug_query and query_type in cls._SHOW_QUERY_FOR_QUERY_TYPES:
            data["result"]["query"] = merged_stream.meta.blocks[0].debug_query  # type: ignore  # TODO: fix
        if totals is not None or totals_query is not None:
            # Note: `totals is None` also happens when everything was filtered out.
            data["result"]["totals"] = totals  # type: ignore  # TODO: fix
            data["result"]["totals_query"] = totals_query  # type: ignore  # TODO: fix
        if data_export_forbidden is not None:
            data["result"]["data_export_forbidden"] = data_export_forbidden  # type: ignore  # TODO: fix
        if fields_data is not None:
            data["result"]["fields"] = fields_data  # type: ignore  # TODO: fix

        return data

    @classmethod
    def make_data_response_v2(
        cls,
        merged_stream: MergedQueryDataStream,
        reporting_registry: Optional[ReportingRegistry] = None,
        data_export_forbidden: Optional[bool] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {}

        if data_export_forbidden is not None:
            data["data_export_forbidden"] = data_export_forbidden

        data["fields"] = merged_stream.legend.items

        data_rows: List[dict] = [{"data": row.data, "legend": row.legend_item_ids} for row in merged_stream.rows]
        data["result_data"] = [{"rows": data_rows}]

        data["blocks"] = cls.make_data_response_v2_block_meta(merged_stream=merged_stream)
        if reporting_registry is not None:
            data["notifications"] = reporting_registry.get_records_of_type(NotificationReportingRecord)

        data = DataApiV2ResponseSchema().dump(data)
        return data


def get_fields_data_raw(dataset: Dataset, for_result: bool = False) -> List[Dict[str, Any]]:
    return [
        {
            "title": fld.title,
            "guid": fld.guid,
            "data_type": fld.data_type,
            "calc_mode": fld.calc_mode,
            **(
                {
                    "hidden": fld.hidden,
                    "type": fld.type,
                }
                if not for_result
                else {}
            ),
        }
        for fld in dataset.result_schema
        if fld.managed_by == ManagedBy.user
    ]


def get_fields_data_serializable(dataset: Dataset, for_result: bool = False) -> List[Dict[str, TJSONLike]]:
    fields = get_fields_data_raw(dataset, for_result=for_result)
    return DatasetFieldsResponseSchema().dump(dict(fields=fields))["fields"]


class PivotDataRequestResponseSerializer(DataRequestResponseSerializerV2Mixin):
    @classmethod
    def make_pivot_response(
        cls,
        merged_stream: MergedQueryDataStream,
        pivot_table: PivotTable,
        reporting_registry: Optional[ReportingRegistry] = None,
    ) -> dict:
        legend_data = LegendSchema().dump(merged_stream.legend)
        pivot_spec_data = {
            "structure": [PivotItemSchema().dump(item) for item in pivot_table.pivot_legend.items],
        }
        # TODO: Dump pivot_table.pivot_legend
        block_meta_list = cls.make_data_response_v2_block_meta(merged_stream=merged_stream)

        def _serialize_cell_vector(value_vector: Optional[DataCellVector]) -> Optional[list[list[Any]]]:
            if value_vector is None:
                return None

            return [
                # [value, legend_item_id, pivot_item_id]
                [pivot_table.unpack_value(cell[0]), cell[1], cell[2]]
                for cell in value_vector.cells
            ]

        table_data = {
            "columns": [
                [_serialize_cell_vector(value_vector) for value_vector in col.values]
                for col in pivot_table.get_columns()
            ],
            "columns_with_info": [
                {
                    "cells": [_serialize_cell_vector(value_vector) for value_vector in col.values],
                    "header_info": PivotHeaderInfoSchema().dump(col.info),
                }
                for col in pivot_table.get_columns()
            ],
            "row_dimension_headers": [
                _serialize_cell_vector(flat_header) for flat_header in pivot_table.get_row_dim_headers()
            ],
            "rows": [
                {
                    "header": [_serialize_cell_vector(value_vector) for value_vector in row.header.values],
                    "header_with_info": {
                        "cells": [_serialize_cell_vector(value_vector) for value_vector in row.header.values],
                        "header_info": PivotHeaderInfoSchema().dump(row.header.info),
                    },
                    "values": [_serialize_cell_vector(value_vector) for value_vector in row.values],
                }
                for row in pivot_table.get_rows()
            ],
        }

        response_data = {
            **legend_data,
            "pivot": pivot_spec_data,
            "pivot_data": table_data,
            "blocks": block_meta_list,
        }

        if reporting_registry is not None:
            response_data["notifications"] = [
                NotificationSchema().dump(notification)
                for notification in reporting_registry.get_records_of_type(NotificationReportingRecord)
            ]
        return response_data

from __future__ import annotations

from http import HTTPStatus
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Union,
)

import attr

from dl_api_client.dsmaker.api.base import HttpApiResponse
from dl_api_client.dsmaker.api.http_async_base import AsyncHttpApiV1Base
from dl_api_client.dsmaker.api.http_sync_base import (
    ClientResponse,
    SyncHttpApiV1Base,
)
from dl_api_client.dsmaker.api.schemas.data import (
    FilterClauseSchema,
    PivotItemSchema,
    PivotPaginationSchema,
    PivotResponseSchema,
    PivotTotalsSchema,
    QueryBlockSchema,
    QueryFieldsItemSchema,
    ResultResponseSchema,
)
from dl_api_client.dsmaker.api.schemas.dataset import WhereClauseSchema
from dl_api_client.dsmaker.api.serialization_base import BaseApiV1SerializationAdapter
from dl_api_client.dsmaker.primitives import (
    BlockSpec,
    Dataset,
    OrderedField,
    ParameterFieldValue,
    PivotPagination,
    PivotTotals,
    RequestLegendItem,
    RequestPivotItem,
    ResultField,
    UpdateAction,
    WhereClause,
)
from dl_constants.enums import (
    FieldRole,
    OrderDirection,
    RangeType,
)


@attr.s(frozen=True)
class HttpDataApiResponse(HttpApiResponse):
    api_version: str = attr.ib(kw_only=True)
    _data: Optional[dict] = attr.ib(kw_only=True, default=None)

    @property
    def data(self) -> dict:
        assert self._data is not None
        return self._data

    @classmethod
    def extract_response_errors(cls, response_json: Dict[str, Any]) -> List[str]:
        errors = super().extract_response_errors(response_json)
        if "message" in response_json:
            errors.append(response_json["message"])
        return errors


@attr.s
class DataApiV1SerializationAdapter(BaseApiV1SerializationAdapter):
    def make_req_data_get_preview(
        self,
        *,
        dataset: Dataset,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
    ) -> Dict[str, Any]:
        data = self.dump_dataset(dataset)
        updates = list(updates or ()) + self.generate_implicit_updates(dataset)  # type: ignore  # TODO: fix
        data["updates"] = self.dump_updates(updates)

        if limit is not None:
            data["limit"] = limit

        return data

    def make_req_data_get_result(
        self,
        *,
        # TODO FIX: Remove after check that frontend team does not use it
        dataset: Optional[Dataset] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[List[ResultField]] = None,
        group_by: Optional[List[ResultField]] = None,
        order_by: Optional[List[Union[ResultField, OrderedField]]] = None,
        where: Optional[List[WhereClause]] = None,
        disable_group_by: Optional[bool] = None,
        with_totals: Optional[bool] = None,
        updates: List[Union[UpdateAction, dict]] = None,
    ) -> Dict[str, Any]:
        if dataset is not None:
            data = self.dump_dataset(dataset)
            updates = list(updates or ()) + self.generate_implicit_updates(dataset)  # type: ignore  # TODO: fix
        else:
            data = {}

        if limit is not None:
            data["limit"] = limit
        if offset is not None:
            data["offset"] = offset
        if fields is not None:
            data["columns"] = [f.id for f in fields]
        if group_by is not None:
            data["group_by"] = [f.id for f in group_by]
        if order_by is not None:
            data["order_by"] = []
            direction_map = {OrderDirection.asc: "ASC", OrderDirection.desc: "DESC"}
            for ob in order_by:
                if isinstance(ob, ResultField):
                    ob = ob.asc
                assert isinstance(ob, OrderedField)
                data["order_by"].append({"column": ob.field_id, "direction": direction_map[ob.direction]})
        if where is not None:
            data["where"] = []
            where_schema = WhereClauseSchema()
            for condition in where:
                data["where"].append(where_schema.dump(condition))
        if updates:
            data["updates"] = self.dump_updates(updates)
        if disable_group_by is not None:
            data["disable_group_by"] = disable_group_by
        if with_totals is not None:
            data["with_totals"] = with_totals

        return data

    def make_req_data_get_value_range(
        self,
        *,
        dataset: Optional[Dataset] = None,
        field: ResultField,
        filters: Optional[List[WhereClause]] = None,
        updates: List[Union[UpdateAction, dict]] = None,
    ) -> Dict[str, Any]:
        if dataset is not None:
            data = self.dump_dataset(dataset)
            updates = list(updates or ()) + self.generate_implicit_updates(dataset)  # type: ignore  # TODO: fix
        else:
            data = {}

        data["field_guid"] = field.id
        if filters is not None:
            data["where"] = []
            where_schema = WhereClauseSchema()
            for condition in filters:
                data["where"].append(where_schema.dump(condition))
        if updates:
            data["updates"] = self.dump_updates(updates)

        return data

    def make_req_data_get_distinct(
        self,
        *,
        dataset: Optional[Dataset] = None,
        limit: Optional[int] = None,
        field: ResultField,
        filters: Optional[List[WhereClause]] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
    ) -> Dict[str, Any]:
        if dataset is not None:
            data = self.dump_dataset(dataset)
            updates = list(updates or ()) + self.generate_implicit_updates(dataset)  # type: ignore  # TODO: fix
        else:
            data = {}

        if limit is not None:
            data["limit"] = limit
        data["field_guid"] = field.id
        if filters is not None:
            data["where"] = []
            where_schema = WhereClauseSchema()
            for condition in filters:
                data["where"].append(where_schema.dump(condition))
        if updates:
            data["updates"] = self.dump_updates(updates)
        if ignore_nonexistent_filters is not None:
            data["ignore_nonexistent_filters"] = ignore_nonexistent_filters

        return data


@attr.s
class DataApiBaseMixin:
    api_v: ClassVar[str]

    def make_response_obj(self, client_response: ClientResponse, data: dict) -> HttpDataApiResponse:
        return HttpDataApiResponse(
            json=client_response.json,
            status_code=client_response.status_code,
            data=data,
            api_version=self.api_v,
        )


@attr.s
class SyncHttpDataApiBase(SyncHttpApiV1Base, DataApiBaseMixin):
    pass


@attr.s
class AsyncHttpDataApiBase(AsyncHttpApiV1Base, DataApiBaseMixin):
    pass


@attr.s
class SyncHttpDataApiV1(SyncHttpDataApiBase):
    api_v = "v1"
    # Override serial_adapter with subclass
    serial_adapter: DataApiV1SerializationAdapter = attr.ib(init=False, factory=DataApiV1SerializationAdapter)

    def get_response_for_dataset_preview(
        self,
        *,
        dataset_id: Optional[str] = None,
        raw_body: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        if dataset_id is None:
            url = f"/api/data/{self.api_v}/datasets/data/preview"
        else:
            url = f"/api/data/{self.api_v}/datasets/{dataset_id}/versions/draft/preview"

        return self._request(url, method="post", data=raw_body or {}, headers=headers)

    def get_response_for_dataset_result(
        self, dataset_id: str, raw_body: dict, headers: Optional[dict] = None
    ) -> ClientResponse:
        """:returns: Flask test client like response"""
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/versions/draft/result"
        return self._request(url, method="post", data=raw_body, headers=headers)

    def get_response_for_dataset_value_range(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        """:returns: Flask test client like response"""
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/versions/draft/values/range"
        return self._request(url, method="post", data=raw_body, headers=headers)

    def get_response_for_dataset_value_distinct(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        """:returns: Flask test client like response"""
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/versions/draft/values/distinct"
        return self._request(url, method="post", data=raw_body, headers=headers)

    def get_preview(
        self,
        *,
        dataset: Dataset,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_preview(
            dataset=dataset,
            updates=updates,
            limit=limit,
        )
        response = self.get_response_for_dataset_preview(
            dataset_id=(dataset.id if dataset.created_ else None),
            raw_body=data,
        )

        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_data = response.json["result"]["data"]
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    def get_result(
        self,
        *,
        dataset: Optional[Dataset] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        offset: int = None,
        fields: Optional[List[ResultField]] = None,
        group_by: Optional[List[ResultField]] = None,
        order_by: Optional[List[Union[ResultField, OrderedField]]] = None,
        where: Optional[List[WhereClause]] = None,  # TODO: Rename -> filters
        filters: Optional[List[WhereClause]] = None,
        disable_group_by: Optional[bool] = None,
        with_totals: Optional[bool] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        filters = filters or where  # For compatibility
        data = self.serial_adapter.make_req_data_get_result(
            dataset=dataset,
            limit=limit,
            offset=offset,
            fields=fields,
            group_by=group_by,
            order_by=order_by,
            where=filters,
            disable_group_by=disable_group_by,
            with_totals=with_totals,
            updates=updates,
        )
        response = self.get_response_for_dataset_result(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_data = response.json["result"]["data"]
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    def get_value_range(
        self,
        *,
        dataset: Optional[Dataset],
        field: ResultField,
        updates: List[Union[UpdateAction, dict]] = None,
        where: Optional[List[WhereClause]] = None,  # TODO: Rename -> filters
        filters: Optional[List[WhereClause]] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        filters = filters or where
        data = self.serial_adapter.make_req_data_get_value_range(
            field=field,
            dataset=dataset,
            filters=filters,
            updates=updates,
        )
        response = self.get_response_for_dataset_value_range(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_data = response.json["result"]["data"]
            except KeyError:
                raise ValueError(response.data)

        return HttpDataApiResponse(
            api_version=self.api_v,
            json=response.json,
            status_code=response.status_code,
            data=resp_data,
        )

    def get_distinct(
        self,
        *,
        dataset: Optional[Dataset] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        field: ResultField,
        where: Optional[List[WhereClause]] = None,  # TODO: Rename -> filters
        filters: Optional[List[WhereClause]] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        filters = filters or where
        data = self.serial_adapter.make_req_data_get_distinct(
            dataset=dataset,
            limit=limit,
            field=field,
            filters=filters,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )
        response = self.get_response_for_dataset_value_distinct(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_data = response.json["result"]["data"]
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    def get_fields(self, dataset_id: str, fail_ok: bool = False) -> HttpDataApiResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/fields"
        response = self._request(url, method="get")
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json

        return self.make_response_obj(client_response=response, data=resp_data)


@attr.s
class DataApiV2SerializationAdapter(BaseApiV1SerializationAdapter):
    def make_req_data_common(
        self,
        *,
        dataset: Optional[Dataset] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[List[Union[ResultField, RequestLegendItem]]] = None,
        order_by: Optional[List[Union[ResultField, OrderedField]]] = None,
        filters: Optional[List[WhereClause]] = None,
        parameters: Optional[List[Union[ResultField, ParameterFieldValue]]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        autofill_legend: bool = False,
        ignore_nonexistent_filters: Optional[bool] = None,
        row_count_hard_limit: Optional[int] = None,
        updates: List[Union[UpdateAction, dict]] = None,
    ) -> Dict[str, Any]:
        if dataset is not None:
            data = self.dump_dataset(dataset)
            updates = list(updates or ()) + self.generate_implicit_updates(dataset)  # type: ignore  # TODO: fix
        else:
            data = {}

        if limit is not None:
            data["limit"] = limit

        if offset is not None:
            data["offset"] = offset

        if fields is not None:
            fields_item_schema = QueryFieldsItemSchema()
            legend_data: List[dict] = []
            for field in fields:
                if isinstance(field, ResultField):
                    field = field.as_req_legend_item(role=FieldRole.row)
                assert isinstance(field, RequestLegendItem)
                legend_data.append(fields_item_schema.dump(field))
            data["fields"] = legend_data

        if order_by is not None:
            order_by_data: List[dict] = []
            for ob_field in order_by:
                if isinstance(ob_field, ResultField):
                    ob_field = ob_field.asc
                assert isinstance(ob_field, OrderedField)
                order_by_data.append(
                    {
                        "ref": {"type": "id", "id": ob_field.field_id},
                        "direction": ob_field.direction.name,
                        "block_id": ob_field.block_id,
                    }
                )
            data["order_by"] = order_by_data

        if filters is not None:
            filter_data: List[dict] = []
            filter_schema = FilterClauseSchema()
            for condition in filters:
                filter_data.append(filter_schema.dump(condition))
            data["filters"] = filter_data

        if parameters is not None:
            parameters_data: List[dict] = []
            for param_field in parameters:
                if isinstance(param_field, ResultField):
                    param_field = param_field.parameter_value()
                assert isinstance(param_field, ParameterFieldValue)
                parameters_data.append(
                    {
                        "ref": {
                            "type": "id",
                            "id": param_field.field_id,
                        },
                        "value": param_field.value,
                        "block_id": param_field.block_id,
                    }
                )
            data["parameter_values"] = parameters_data

        if blocks is not None:
            block_data: List[dict] = []
            block_schema = QueryBlockSchema()
            for block in blocks:
                block_data.append(block_schema.dump(block))
            data["blocks"] = block_data

        if updates:
            data["updates"] = self.dump_updates(updates)

        data["autofill_legend"] = autofill_legend

        if ignore_nonexistent_filters is not None:
            data["ignore_nonexistent_filters"] = ignore_nonexistent_filters

        if row_count_hard_limit is not None:
            data["row_count_hard_limit"] = row_count_hard_limit

        return data

    def make_req_data_get_preview(
        self,
        *,
        dataset: Dataset,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
    ) -> Dict[str, Any]:
        data = self.dump_dataset(dataset)
        updates = list(updates or ()) + self.generate_implicit_updates(dataset)
        data["updates"] = self.dump_updates(updates)

        if limit is not None:
            data["limit"] = limit

        return data

    def make_req_data_get_result(
        self,
        *,
        dataset: Optional[Dataset] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[List[Union[ResultField, RequestLegendItem]]] = None,
        order_by: Optional[List[Union[ResultField, OrderedField]]] = None,
        filters: Optional[List[WhereClause]] = None,
        parameters: Optional[List[Union[ResultField, ParameterFieldValue]]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        disable_group_by: Optional[bool] = None,
        with_totals: Optional[bool] = None,
        result_with_totals: Optional[bool] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        row_count_hard_limit: Optional[int] = None,
        updates: List[Union[UpdateAction, dict]] = None,
    ) -> Dict[str, Any]:
        data = self.make_req_data_common(
            dataset=dataset,
            limit=limit,
            offset=offset,
            fields=fields,
            order_by=order_by,
            filters=filters,
            parameters=parameters,
            blocks=blocks,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
            row_count_hard_limit=row_count_hard_limit,
        )
        if disable_group_by is not None:
            data["disable_group_by"] = disable_group_by
        if with_totals is not None:
            data["with_totals"] = with_totals
        if result_with_totals is not None:
            data["result"] = {}
            data["result"]["with_totals"] = with_totals

        return data

    def make_req_data_get_value_range(
        self,
        *,
        dataset: Optional[Dataset] = None,
        field: ResultField,
        filters: Optional[List[WhereClause]] = None,
        parameters: Optional[List[Union[ResultField, ParameterFieldValue]]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        updates: List[Union[UpdateAction, dict]] = None,
    ) -> Dict[str, Any]:
        fields = [
            field.as_req_legend_item(role=FieldRole.range, range_type=RangeType.min),
            field.as_req_legend_item(role=FieldRole.range, range_type=RangeType.max),
        ]
        data = self.make_req_data_common(
            dataset=dataset,
            fields=fields,
            filters=filters,
            parameters=parameters,
            blocks=blocks,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )

        return data

    def make_req_data_get_distinct(
        self,
        *,
        dataset: Optional[Dataset] = None,
        field: Union[ResultField, RequestLegendItem],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[List[Union[ResultField, OrderedField]]] = None,
        filters: Optional[List[WhereClause]] = None,
        parameters: Optional[List[Union[ResultField, ParameterFieldValue]]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        updates: List[Union[UpdateAction, dict]] = None,
    ) -> Dict[str, Any]:
        field_li: RequestLegendItem
        if isinstance(field, RequestLegendItem):
            assert field.role_spec.role == FieldRole.distinct
            field_li = field
        else:
            field_li = field.as_req_legend_item(role=FieldRole.distinct)
        fields = [field_li]
        data = self.make_req_data_common(
            dataset=dataset,
            limit=limit,
            offset=offset,
            fields=fields,
            order_by=order_by,
            filters=filters,
            parameters=parameters,
            blocks=blocks,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )

        return data

    def make_req_data_get_pivot(
        self,
        *,
        dataset: Optional[Dataset] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[List[Union[ResultField, RequestLegendItem]]] = None,
        order_by: Optional[List[OrderedField]] = None,
        filters: Optional[List[WhereClause]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        autofill_legend: bool = False,
        ignore_nonexistent_filters: Optional[bool] = None,
        pivot_pagination: Optional[PivotPagination] = None,
        pivot_structure: Optional[list[RequestPivotItem]] = None,
        pivot_totals: Optional[PivotTotals] = None,
        with_totals: Optional[bool] = None,
        updates: List[Union[UpdateAction, dict]] = None,
    ) -> Dict[str, Any]:
        data = self.make_req_data_common(
            dataset=dataset,
            limit=limit,
            offset=offset,
            fields=fields,
            order_by=order_by,
            filters=filters,
            blocks=blocks,
            autofill_legend=autofill_legend,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )
        data["pivot"] = {}
        if pivot_structure is not None:
            data["pivot"]["structure"] = [PivotItemSchema().dump(item) for item in pivot_structure]
        if pivot_pagination is not None:
            data["pivot"]["pagination"] = PivotPaginationSchema().dump(pivot_pagination)
        if with_totals is not None:
            data["pivot"]["with_totals"] = with_totals
        if pivot_totals is not None:
            data["pivot"]["totals"] = PivotTotalsSchema().dump(pivot_totals)

        return data


@attr.s
class SyncHttpDataApiV2(SyncHttpDataApiBase):
    api_v = "v2"
    # Override serial_adapter with subclass
    serial_adapter: DataApiV2SerializationAdapter = attr.ib(init=False, factory=DataApiV2SerializationAdapter)

    def _make_resp_data(self, full_response_data: dict) -> dict:
        return full_response_data

    def get_response_for_dataset_preview(
        self,
        dataset_id: Optional[str],
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        if dataset_id is None:
            url = f"/api/data/{self.api_v}/datasets/data/preview"
        else:
            url = f"/api/data/{self.api_v}/datasets/{dataset_id}/preview"
        return self._request(url, method="post", data=raw_body, headers=headers)

    def get_response_for_dataset_result(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/result"
        return self._request(url, method="post", data=raw_body, headers=headers)

    def get_response_for_dataset_value_range(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/values/range"
        return self._request(url, method="post", data=raw_body, headers=headers)

    def get_response_for_dataset_value_distinct(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/values/distinct"
        return self._request(url, method="post", data=raw_body, headers=headers)

    def get_response_for_dataset_pivot(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/pivot"
        return self._request(url, method="post", data=raw_body, headers=headers)

    def get_preview(
        self,
        *,
        dataset: Dataset,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_preview(dataset=dataset, limit=limit, updates=updates)
        response = self.get_response_for_dataset_preview(
            dataset_id=(dataset.id if dataset.created_ else None),
            raw_body=data,
        )
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_schema = ResultResponseSchema()
                resp_data = resp_schema.load(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    def get_result(
        self,
        *,
        dataset: Optional[Dataset] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[List[Union[ResultField, RequestLegendItem]]] = None,
        order_by: Optional[List[Union[ResultField, OrderedField]]] = None,
        filters: Optional[List[WhereClause]] = None,
        parameters: Optional[List[Union[ResultField, ParameterFieldValue]]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        disable_group_by: Optional[bool] = None,
        with_totals: Optional[bool] = None,
        result_with_totals: Optional[bool] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        row_count_hard_limit: Optional[int] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_result(
            dataset=dataset,
            limit=limit,
            offset=offset,
            fields=fields,
            order_by=order_by,
            filters=filters,
            parameters=parameters,
            blocks=blocks,
            disable_group_by=disable_group_by,
            with_totals=with_totals,
            result_with_totals=result_with_totals,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
            row_count_hard_limit=row_count_hard_limit,
            updates=updates,
        )
        response = self.get_response_for_dataset_result(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_schema = ResultResponseSchema()
                resp_data = resp_schema.load(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    def get_value_range(
        self,
        *,
        dataset: Optional[Dataset],
        field: ResultField,
        updates: List[Union[UpdateAction, dict]] = None,
        filters: Optional[List[WhereClause]] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_value_range(
            field=field,
            dataset=dataset,
            filters=filters,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )
        response = self.get_response_for_dataset_value_range(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_schema = ResultResponseSchema()
                resp_data = resp_schema.load(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    def get_distinct(
        self,
        *,
        dataset: Optional[Dataset] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        field: ResultField,
        filters: Optional[List[WhereClause]] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_distinct(
            dataset=dataset,
            limit=limit,
            field=field,
            filters=filters,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )
        response = self.get_response_for_dataset_value_distinct(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_schema = ResultResponseSchema()
                resp_data = resp_schema.load(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    def get_pivot(
        self,
        *,
        dataset: Optional[Dataset] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        offset: Optional[int] = None,
        fields: Optional[List[Union[ResultField, RequestLegendItem]]] = None,
        order_by: Optional[List[OrderedField]] = None,
        filters: Optional[List[WhereClause]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        autofill_legend: bool = False,
        pivot_structure: Optional[list[RequestPivotItem]] = None,
        pivot_pagination: Optional[PivotPagination] = None,
        pivot_totals: Optional[PivotTotals] = None,
        with_totals: Optional[bool] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_pivot(
            dataset=dataset,
            limit=limit,
            offset=offset,
            fields=fields,
            order_by=order_by,
            filters=filters,
            blocks=blocks,
            updates=updates,
            autofill_legend=autofill_legend,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
            pivot_structure=pivot_structure,
            pivot_pagination=pivot_pagination,
            pivot_totals=pivot_totals,
            with_totals=with_totals,
        )
        response = self.get_response_for_dataset_pivot(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_schema = PivotResponseSchema()
                resp_data = resp_schema.load(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)


@attr.s
class AsyncHttpDataApiV2(AsyncHttpDataApiBase):
    api_v = "v2"
    # Override serial_adapter with subclass
    serial_adapter: DataApiV2SerializationAdapter = attr.ib(init=False, factory=DataApiV2SerializationAdapter)

    def _make_resp_data(self, full_response_data: dict) -> dict:
        return full_response_data

    async def get_response_for_dataset_preview(
        self,
        dataset_id: Optional[str],
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        if dataset_id is None:
            url = f"/api/data/{self.api_v}/datasets/data/preview"
        else:
            url = f"/api/data/{self.api_v}/datasets/{dataset_id}/preview"
        return await self._request(url, method="post", data=raw_body, headers=headers)

    async def get_response_for_dataset_result(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/result"
        return await self._request(url, method="post", data=raw_body, headers=headers)

    async def get_response_for_dataset_value_range(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/values/range"
        return await self._request(url, method="post", data=raw_body, headers=headers)

    async def get_response_for_dataset_value_distinct(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/values/distinct"
        return await self._request(url, method="post", data=raw_body, headers=headers)

    async def get_response_for_dataset_pivot(
        self,
        dataset_id: str,
        raw_body: dict,
        headers: Optional[dict] = None,
    ) -> ClientResponse:
        url = f"/api/data/{self.api_v}/datasets/{dataset_id}/pivot"
        return await self._request(url, method="post", data=raw_body, headers=headers)

    async def get_preview(
        self,
        *,
        dataset: Dataset,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_preview(dataset=dataset, limit=limit, updates=updates)
        response = await self.get_response_for_dataset_preview(
            dataset_id=(dataset.id if dataset.created_ else None),
            raw_body=data,
        )
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_data = self._make_resp_data(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    async def get_result(
        self,
        *,
        dataset: Optional[Dataset] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[List[Union[ResultField, RequestLegendItem]]] = None,
        order_by: Optional[List[Union[ResultField, OrderedField]]] = None,
        filters: Optional[List[WhereClause]] = None,
        parameters: Optional[List[Union[ResultField, ParameterFieldValue]]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        disable_group_by: Optional[bool] = None,
        with_totals: Optional[bool] = None,
        result_with_totals: Optional[bool] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        row_count_hard_limit: Optional[int] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_result(
            dataset=dataset,
            limit=limit,
            offset=offset,
            fields=fields,
            order_by=order_by,
            filters=filters,
            parameters=parameters,
            blocks=blocks,
            disable_group_by=disable_group_by,
            with_totals=with_totals,
            result_with_totals=result_with_totals,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
            row_count_hard_limit=row_count_hard_limit,
            updates=updates,
        )
        response = await self.get_response_for_dataset_result(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_data = self._make_resp_data(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    async def get_value_range(
        self,
        *,
        dataset: Optional[Dataset],
        field: ResultField,
        updates: List[Union[UpdateAction, dict]] = None,
        filters: Optional[List[WhereClause]] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_value_range(
            field=field,
            dataset=dataset,
            filters=filters,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )
        response = await self.get_response_for_dataset_value_range(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_data = self._make_resp_data(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    async def get_distinct(
        self,
        *,
        dataset: Optional[Dataset] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        field: ResultField,
        filters: Optional[List[WhereClause]] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_distinct(
            dataset=dataset,
            limit=limit,
            field=field,
            filters=filters,
            updates=updates,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )
        response = await self.get_response_for_dataset_value_distinct(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_data = self._make_resp_data(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)

    async def get_pivot(
        self,
        *,
        dataset: Optional[Dataset] = None,
        updates: List[Union[UpdateAction, dict]] = None,
        limit: int = None,
        offset: Optional[int] = None,
        fields: Optional[List[Union[ResultField, RequestLegendItem]]] = None,
        order_by: Optional[List[OrderedField]] = None,
        filters: Optional[List[WhereClause]] = None,
        blocks: Optional[List[BlockSpec]] = None,
        autofill_legend: bool = False,
        pivot_structure: Optional[list[RequestPivotItem]] = None,
        pivot_pagination: Optional[PivotPagination] = None,
        pivot_totals: Optional[PivotTotals] = None,
        with_totals: Optional[bool] = None,
        ignore_nonexistent_filters: Optional[bool] = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        data = self.serial_adapter.make_req_data_get_pivot(
            dataset=dataset,
            limit=limit,
            offset=offset,
            fields=fields,
            order_by=order_by,
            filters=filters,
            blocks=blocks,
            updates=updates,
            autofill_legend=autofill_legend,
            ignore_nonexistent_filters=ignore_nonexistent_filters,
            pivot_structure=pivot_structure,
            pivot_pagination=pivot_pagination,
            pivot_totals=pivot_totals,
            with_totals=with_totals,
        )
        response = await self.get_response_for_dataset_pivot(dataset_id=dataset.id, raw_body=data)
        resp_data: Optional[dict] = None
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        if response.status_code == HTTPStatus.OK:
            try:
                resp_schema = PivotResponseSchema()
                resp_data = resp_schema.load(response.json)
            except KeyError:
                raise ValueError(response.data)

        return self.make_response_obj(client_response=response, data=resp_data)


@attr.s
class SyncHttpDataApiV1_5(SyncHttpDataApiV2):
    api_v = "v1.5"

    def _make_resp_data(self, full_response_data: dict) -> dict:
        return full_response_data["result"]["data"]

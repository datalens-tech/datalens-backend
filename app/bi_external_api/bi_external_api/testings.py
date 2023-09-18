from __future__ import annotations

import abc
from typing import Optional, ClassVar, TypeVar, Sequence, Type, Union

import attr

from dl_api_commons.base_models import TenantCommon
from dl_api_commons.client.base import Req, DLCommonAPIClient
from dl_constants.enums import CalcMode, AggregationFunction, BIType, FieldType, ManagedBy
from bi_external_api.converter.charts.utils import convert_field_type_dataset_to_chart
from bi_external_api.domain import external as ext
from bi_external_api.domain.external import get_external_model_mapper
from bi_external_api.domain.external import rpc_dc
from bi_external_api.domain.internal import (
    charts,
    datasets,
    dashboards,
    dl_common,
)
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.dataset_api import APIClientBIBackControlPlane
from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq
from bi_external_api.structs.singleormultistring import SingleOrMultiString
from bi_external_api.workbook_ops.public_exceptions import WorkbookOperationException


@attr.s(auto_attribs=True)
class SingleAvatarDatasetBuilder:
    _dsrc: Optional[datasets.DataSource] = attr.ib(default=None)
    _fields: list[datasets.ResultSchemaFieldFull] = attr.ib(init=False, factory=list)
    _entry_summary: Optional[dl_common.EntrySummary] = attr.ib(init=False, default=None)

    _single_avatar_id: ClassVar[str] = "the_avatar"
    _single_avatar_title: ClassVar[str] = "THE AVATAR"
    _single_source_id: ClassVar[str] = "the_source"
    _single_source_title: ClassVar[str] = "THE SOURCE"

    def summary(self, *, id: str, name: str, wb_id: str) -> SingleAvatarDatasetBuilder:
        self._entry_summary = dl_common.EntrySummary(
            id=id,
            name=name,
            workbook_id=wb_id,
            scope=dl_common.EntryScope.dataset,
        )
        return self

    def pg_subselect_dsrc(self, query: str, conn_id: str) -> SingleAvatarDatasetBuilder:
        self._dsrc = datasets.DataSourcePGSubSQL(
            connection_id=conn_id,
            id=self._single_source_id,
            title=self._single_source_title,
            parameters=datasets.DataSourceParamsSubSQL(subsql=query),
        )
        return self

    def fake_pg_table(self, conn_id: str) -> SingleAvatarDatasetBuilder:
        self._dsrc = datasets.DataSourcePGTable(
            connection_id=conn_id,
            id=self._single_source_id,
            title=self._single_source_title,
            parameters=datasets.DataSourceParamsSchematizedSQL(
                db_name="fake_db",
                db_version="12.4",
                schema_name="public",
                table_name="fake_schema",
            )
        )
        return self

    def df(
            self, *,
            id: str, data_type: BIType,
            src: Optional[str] = None,
            f_type: Optional[FieldType] = None, aggregation: Optional[AggregationFunction] = None
    ) -> SingleAvatarDatasetBuilder:
        effective_src = id if src is None else src
        effective_agg = AggregationFunction.none if aggregation is None else aggregation
        effective_f_type = f_type if f_type is not None else (
            FieldType.MEASURE if effective_agg != AggregationFunction.none else FieldType.DIMENSION
        )

        field = datasets.ResultSchemaFieldFull(
            guid=id,
            title=id,
            description=f"{id=}",
            #
            calc_mode=CalcMode.direct,
            avatar_id=self._single_avatar_id,
            source=effective_src,
            formula="",
            #
            aggregation=effective_agg,
            type=effective_f_type,
            cast=data_type,
            data_type=data_type,
            initial_data_type=data_type,
            #
            strict=False,
            hidden=False,
            has_auto_aggregation=False,
            lock_aggregation=False,
            aggregation_locked=False,
            autoaggregated=False,
            managed_by=ManagedBy.user,
            virtual=False,
            valid=True
        )
        self._fields.append(field)
        return self

    def ff(
            self, *,
            id: str, data_type: BIType, formula: str,
            f_type: Optional[FieldType] = None, aggregation: Optional[AggregationFunction] = None
    ) -> SingleAvatarDatasetBuilder:
        effective_agg = AggregationFunction.none if aggregation is None else aggregation
        effective_f_type = f_type if f_type is not None else (
            FieldType.MEASURE if effective_agg != AggregationFunction.none else FieldType.DIMENSION
        )

        field = datasets.ResultSchemaFieldFull(
            guid=id,
            title=id,
            description=f"{id=}",
            #
            calc_mode=CalcMode.formula,
            avatar_id="",
            source="",
            formula=formula,
            #
            aggregation=effective_agg,
            type=effective_f_type,
            cast=data_type,
            data_type=data_type,
            initial_data_type=data_type,
            #
            strict=False,
            hidden=False,
            has_auto_aggregation=False,
            lock_aggregation=False,
            aggregation_locked=False,
            autoaggregated=False,
            managed_by=ManagedBy.user,
            virtual=False,
            valid=True
        )

        self._fields.append(field)
        return self

    def build_dataset(self) -> datasets.Dataset:
        dsrc = self._dsrc
        assert dsrc is not None, "Data source was not defined"
        avatar = datasets.Avatar(
            is_root=True,
            id=self._single_avatar_id,
            source_id=self._single_source_id,
            title=self._single_avatar_title,
        )

        return datasets.Dataset(
            sources=[dsrc],
            source_avatars=[avatar],
            result_schema=list(self._fields),
        )

    def build_dataset_instance(self) -> datasets.DatasetInstance:
        summary = self._entry_summary
        assert summary is not None

        return datasets.DatasetInstance(
            summary=summary,
            dataset=self.build_dataset(),
        )


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class _ChartExtraFormulaField:
    id: str
    title: str
    data_type: BIType
    formula: str
    f_type: FieldType
    aggregation: AggregationFunction


_CHART_BUILDER_TV = TypeVar("_CHART_BUILDER_TV", bound="ChartBuilder")


@attr.s(kw_only=True)
class ChartBuilder(metaclass=abc.ABCMeta):
    _dataset_inst: Optional[datasets.DatasetInstance] = attr.ib(default=None)
    _extra_fields: list[_ChartExtraFormulaField] = attr.ib(init=False, factory=list)

    @property
    def _dataset(self) -> datasets.Dataset:
        assert self._dataset_inst is not None
        return self._dataset_inst.dataset

    @property
    def _dataset_id(self) -> str:
        assert self._dataset_inst is not None
        return self._dataset_inst.summary.id

    def _rs_field_to_chart_field(self, rs_field: datasets.ResultSchemaFieldFull) -> charts.ChartField:
        return charts.ChartField(
            data_type=rs_field.data_type,
            title=rs_field.title,
            guid=rs_field.guid,
            datasetId=self._dataset_id,
            type=convert_field_type_dataset_to_chart(
                rs_field.type
            ),
        )

    def _chart_extra_formula_field_to_chart_field(self, extra: _ChartExtraFormulaField) -> charts.ChartField:
        return charts.ChartField(
            data_type=extra.data_type,
            title=extra.id,
            guid=extra.id,
            datasetId=self._dataset_id,
            type=convert_field_type_dataset_to_chart(extra.f_type),
        )

    def _get_chart_field_by_id(self, id: str) -> charts.ChartField:
        extra_field = next(
            (extra for extra in self._extra_fields if extra.id == id), None
        )
        if extra_field is not None:
            return self._chart_extra_formula_field_to_chart_field(extra_field)

        return self._rs_field_to_chart_field(self._dataset.get_field_by_id(id))

    def extra_ff(
            self: _CHART_BUILDER_TV, *,
            id: str, title: Optional[str] = None,
            data_type: BIType, formula: str,
            f_type: Optional[FieldType] = None, aggregation: Optional[AggregationFunction] = None,
    ) -> _CHART_BUILDER_TV:
        effective_agg = AggregationFunction.none if aggregation is None else aggregation
        effective_f_type = f_type if f_type is not None else (
            FieldType.MEASURE if effective_agg != AggregationFunction.none else FieldType.DIMENSION
        )
        self._extra_fields.append(_ChartExtraFormulaField(
            id=id,
            title=id if title is None else title,
            data_type=data_type,
            formula=formula,
            f_type=effective_f_type,
            aggregation=effective_agg,
        ))
        return self

    def dataset_instance(self: _CHART_BUILDER_TV, dataset_inst: datasets.DatasetInstance) -> _CHART_BUILDER_TV:
        self._dataset_inst = dataset_inst
        return self

    @abc.abstractmethod
    def build_chart(self) -> charts.Chart:
        raise NotImplementedError()

    def build_chart_instance(self, *, id: str, name: str, wb_id: str) -> charts.ChartInstance:
        return charts.ChartInstance(
            chart=self.build_chart(),
            summary=dl_common.EntrySummary(
                scope=dl_common.EntryScope.widget,
                name=name,
                id=id,
                workbook_id=wb_id,
            )
        )


_FLAT_TABLE_CHART_BUILDER_TV = TypeVar("_FLAT_TABLE_CHART_BUILDER_TV", bound="FlatTableChartBuilder")


@attr.s(kw_only=True)
class FlatTableChartBuilder(ChartBuilder):
    _column_ids: list[str] = attr.ib(factory=list)

    def all_dataset_fields_to_columns(self: _FLAT_TABLE_CHART_BUILDER_TV) -> _FLAT_TABLE_CHART_BUILDER_TV:
        self._column_ids.extend(
            [rs.guid for rs in self._dataset.result_schema]
        )
        return self

    def dataset_fields_to_columns(self: _FLAT_TABLE_CHART_BUILDER_TV, *col_ids: str) -> _FLAT_TABLE_CHART_BUILDER_TV:
        self._column_ids.extend(col_ids)
        return self

    def build_chart(self) -> charts.Chart:
        partials: list[charts.DatasetFieldPartial] = []
        partials.extend(
            charts.DatasetFieldPartial(title=rs_field.title, guid=rs_field.guid)
            for rs_field in self._dataset.result_schema
        )
        partials.extend(
            charts.DatasetFieldPartial(title=extra.title, guid=extra.id)
            for extra in self._extra_fields
        )

        return charts.Chart(
            colors=[],
            filters=[],
            visualization=charts.Visualization(
                id=charts.VisualizationId.flatTable,
                placeholders=[
                    charts.Placeholder(
                        id=charts.PlaceholderId.FlatTableColumns,
                        items=[self._get_chart_field_by_id(cid) for cid in self._column_ids],
                    )
                ],
            ),
            datasetsIds=[self._dataset_id],
            datasetsPartialFields=[partials],
        )


@attr.s(kw_only=True)
class SingleTabDashboardBuilder:
    @attr.s(kw_only=True, auto_attribs=True)
    class PP:
        x: int
        y: int
        h: int
        w: int

    _items: list[dashboards.TabItem] = attr.ib(factory=list)
    _placements: list[dashboards.LayoutItem] = attr.ib(factory=list)
    _tab_title: str = attr.ib(default="some tab title")
    _tab_id: str = attr.ib(default="some_tab_id")

    def _add_pp(self, pp: PP, tab_item_id: str) -> None:
        self._placements.append(
            dashboards.LayoutItem(
                i=tab_item_id,
                h=pp.h,
                w=pp.w,
                x=pp.x,
                y=pp.y,
            ),
        )

    def tab_title(self, title: str) -> SingleTabDashboardBuilder:
        self._tab_title = title
        return self

    def tab_id(self, tab_id: str) -> SingleTabDashboardBuilder:
        self._tab_id = tab_id
        return self

    def item_widget_single_tab(
            self, *,
            dash_tab_item_id: str,
            title: str,
            pp: PP,
            #
            chart_id: str,
            widget_tab_item_id: str,
    ) -> SingleTabDashboardBuilder:
        self._items.append(
            dashboards.ItemWidget(
                id=dash_tab_item_id,
                namespace='default',
                data=dashboards.TabItemDataWidget(
                    hideTitle=False,
                    tabs=(
                        dashboards.WidgetTabItem(
                            id=widget_tab_item_id,
                            title=title,
                            params=FrozenMappingStrToStrOrStrSeq({}),
                            chartId=chart_id,
                            isDefault=True,
                            autoHeight=False,
                            description='',
                        ),
                    ),
                ),
            )
        )
        self._add_pp(pp, dash_tab_item_id)
        return self

    def item_selector_dataset_based(
            self, *,
            dash_tab_item_id: str,
            title: str,
            pp: PP,
            #
            dataset_inst: datasets.DatasetInstance,
            field_id: str,
            default_values: Sequence[str],
    ) -> SingleTabDashboardBuilder:
        field = dataset_inst.dataset.get_field_by_id(field_id)
        self._items.append(
            dashboards.ItemControl(
                id=dash_tab_item_id,
                namespace='default',
                data=dashboards.DatasetBasedControlData(
                    title=title,
                    source=dashboards.DatasetControlSourceSelect(
                        showTitle=True,
                        datasetId=dataset_inst.summary.id,
                        datasetFieldId=field_id,
                        datasetFieldType=convert_field_type_dataset_to_chart(field.type),
                        fieldType=field.data_type,
                        multiselectable=True,
                        defaultValue=SingleOrMultiString.from_sequence(default_values),
                    ),
                ),
                defaults=FrozenMappingStrToStrOrStrSeq({
                    field_id: tuple(default_values),
                }),
            ),
        )
        self._add_pp(pp, dash_tab_item_id)
        return self

    def build_dash(self) -> dashboards.Dashboard:
        return dashboards.Dashboard(
            tabs=(
                dashboards.Tab(
                    id=self._tab_id,
                    title=self._tab_title,
                    items=tuple(self._items),
                    layout=tuple(self._placements),
                    connections=(),
                    aliases=dashboards.Aliases(
                        default=(),
                    ),
                ),
            ),
        )


@attr.s(auto_attribs=True, frozen=True)
class PGSubSelectDatasetFactory:
    conn_id: str
    bi_api_cli: APIClientBIBackControlPlane
    wb_id: str

    async def create_dataset(self, ds_name: str, *, query: str, ) -> datasets.DatasetInstance:
        source = datasets.DataSourcePGSubSQL(
            id="src",
            title="SRC",
            connection_id=self.conn_id,
            parameters=datasets.DataSourceParamsSubSQL(subsql=query)
        )
        avatar = datasets.Avatar(
            is_root=True,
            id="src",
            title="SRC",
            source_id=source.id,
        )
        actions = [
            datasets.ActionDataSourceAdd(source=source),
            datasets.ActionAvatarAdd(source_avatar=avatar, disable_fields_update=False),
        ]
        temporary_dataset, _ = await self.bi_api_cli.build_dataset_config_by_actions(actions)

        final_actions = [
            datasets.ActionDataSourceAdd(source=source),
            datasets.ActionAvatarAdd(source_avatar=avatar, disable_fields_update=True),
        ]

        for idx, generated_field in enumerate(temporary_dataset.result_schema):
            tuned_field = attr.evolve(
                generated_field.to_writable_result_schema(),
                guid=generated_field.title,
                # TODO FIX: Check if not a temporary bug in int preprod
                #  (validation returns formula = None for direct fields,
                #  but on attempt to revalidate dataset dataset we got:
                #  `{'updates': {2: {'field': {'formula': ['Field may not be null.']}}`)
                formula=generated_field.formula if generated_field.formula is not None else "",
            )
            final_actions.append(datasets.ActionFieldAdd(field=tuned_field, order_index=idx))

        final_dataset, validation_resp = await self.bi_api_cli.build_dataset_config_by_actions(final_actions)
        dataset_summary = await self.bi_api_cli.create_dataset(validation_resp, workbook_id=self.wb_id, name=ds_name)
        dataset = await self.bi_api_cli.get_dataset(dataset_summary.id)

        return datasets.DatasetInstance(summary=dataset_summary, dataset=dataset)


_RPC_RESP_TV = TypeVar("_RPC_RESP_TV", bound=Union[ext.WorkbookOpResponse, rpc_dc.DCOpResponse])


@attr.s(kw_only=True)
class WorkbookOpsClient(DLCommonAPIClient):
    _api_type: ExtAPIType = attr.ib()

    _ADD_SECRETS_TO = [
        # todo: think about better solution instead of this workaround ...
        ext.ConnectionCreateRequest,
        ext.ConnectionModifyRequest,

        rpc_dc.DCOpConnectionCreateRequest,
        rpc_dc.DCOpConnectionModifyRequest,
    ]

    def __attrs_post_init__(self) -> None:
        assert isinstance(self._tenant, TenantCommon),\
            "WorkbookOpsClient should not have stick tenant due API nature."

    async def _execute_op(
            self,
            op_request: ext.WorkbookOpRequest | rpc_dc.DCOpRequest,
            resp_clz: Type[_RPC_RESP_TV],
    ) -> _RPC_RESP_TV:

        mapper = get_external_model_mapper(self._api_type)
        if isinstance(op_request, ext.WorkbookOpRequest):
            req_schema = mapper.get_schema_for_attrs_class(ext.WorkbookOpRequest)()
        else:
            req_schema = mapper.get_schema_for_attrs_class(rpc_dc.DCOpRequest)()

        resp_schema = mapper.get_schema_for_attrs_class(resp_clz)()

        err_schema = mapper.get_or_create_schema_for_attrs_class(ext.ErrWorkbookOp)()

        req_json = req_schema.dump(op_request)
        if isinstance(op_request, tuple(self._ADD_SECRETS_TO)) and hasattr(op_request, "secret"):
            con_sec = op_request.secret  # type: ignore
            if isinstance(con_sec, ext.PlainSecret):
                req_json["secret"]["secret"] = con_sec.secret

        if isinstance(op_request, ext.FakeWorkbookCreateRequest) and hasattr(op_request, "connection_secrets"):
            conn_secrets = op_request.connection_secrets
            if conn_secrets:
                for conn_sec_obj, sec_dict in zip(conn_secrets, req_json["connection_secrets"]):
                    secret_value_obj = conn_sec_obj.secret
                    if isinstance(secret_value_obj, ext.PlainSecret):
                        sec_dict["secret"]["secret"] = secret_value_obj.secret

        resp = await self.make_request(Req(
            method="POST",
            url="/external_api/v0/workbook/rpc",
            data_json=req_json,
            require_ok=False,
        ))

        if resp.status == 200:
            app_resp = resp_schema.load(resp.json)
            assert isinstance(app_resp, resp_clz)
            return app_resp
        elif resp.status == 400:
            # TODO FIX: Cleanup
            if resp.json.get("kind") == "request_scheme_violation":
                raise AssertionError("Error in client: serialized request did not match server schema", resp.json)

            try:
                err_data = err_schema.load(resp.json)
            except Exception as exc:
                raise ValueError("Can not deserialize error message", resp.json) from exc

            raise WorkbookOperationException(
                err_data
            )
        else:
            raise AssertionError(f"GOT UNEXPECTED STATUS CODE {resp.status}\n{resp.json}")

    async def create_fake_workbook(self, rq: ext.FakeWorkbookCreateRequest) -> ext.FakeWorkbookCreateResponse:
        return await self._execute_op(rq, ext.FakeWorkbookCreateResponse)

    async def create_true_workbook(self, rq: ext.TrueWorkbookCreateRequest) -> ext.TrueWorkbookCreateResponse:
        return await self._execute_op(rq, ext.TrueWorkbookCreateResponse)

    async def create_connection(self, rq: ext.ConnectionCreateRequest) -> ext.ConnectionCreateResponse:
        return await self._execute_op(rq, ext.ConnectionCreateResponse)

    async def write_workbook(self, rq: ext.WorkbookWriteRequest) -> ext.WorkbookWriteResponse:
        return await self._execute_op(rq, ext.WorkbookWriteResponse)

    async def read_workbook(self, rq: ext.WorkbookReadRequest) -> ext.WorkbookReadResponse:
        return await self._execute_op(rq, ext.WorkbookReadResponse)

    async def delete_workbook(self, rq: ext.WorkbookDeleteRequest) -> ext.WorkbookDeleteResponse:
        return await self._execute_op(rq, ext.WorkbookDeleteResponse)

    async def advise_dataset_fields(self, rq: ext.AdviseDatasetFieldsRequest) -> ext.AdviseDatasetFieldsResponse:
        return await self._execute_op(rq, ext.AdviseDatasetFieldsResponse)

    async def dc_create_workbook(self, rq: rpc_dc.DCOpWorkbookCreateRequest) -> rpc_dc.DCOpWorkbookCreateResponse:
        return await self._execute_op(rq, rpc_dc.DCOpWorkbookCreateResponse)

    async def dc_modify_workbook(self, rq: rpc_dc.DCOpWorkbookModifyRequest) -> rpc_dc.DCOpWorkbookModifyResponse:
        return await self._execute_op(rq, rpc_dc.DCOpWorkbookModifyResponse)

    async def dc_read_workbook(self, rq: rpc_dc.DCOpWorkbookGetRequest) -> rpc_dc.DCOpWorkbookGetResponse:
        return await self._execute_op(rq, rpc_dc.DCOpWorkbookGetResponse)

    async def dc_delete_workbook(self, rq: rpc_dc.DCOpWorkbookDeleteRequest) -> rpc_dc.DCOpWorkbookDeleteResponse:
        return await self._execute_op(rq, rpc_dc.DCOpWorkbookDeleteResponse)

    async def dc_connection_create(self, rq: rpc_dc.DCOpConnectionCreateRequest) -> rpc_dc.DCOpConnectionCreateResponse:
        return await self._execute_op(rq, rpc_dc.DCOpConnectionCreateResponse)

    async def dc_connection_get(self, rq: rpc_dc.DCOpConnectionGetRequest) -> rpc_dc.DCOpConnectionGetResponse:
        return await self._execute_op(rq, rpc_dc.DCOpConnectionGetResponse)

    async def dc_connection_modify(self, rq: rpc_dc.DCOpConnectionModifyRequest) -> rpc_dc.DCOpConnectionModifyResponse:
        return await self._execute_op(rq, rpc_dc.DCOpConnectionModifyResponse)

    async def dc_connection_delete(self, rq: rpc_dc.DCOpConnectionDeleteRequest) -> rpc_dc.DCOpConnectionDeleteResponse:
        return await self._execute_op(rq, rpc_dc.DCOpConnectionDeleteResponse)

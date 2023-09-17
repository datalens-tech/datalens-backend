import re
from contextlib import contextmanager
from typing import Type, Sequence, Optional, Iterator

import attr

from bi_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE

from bi_external_api.converter.charts.chart_converter import BaseChartConverter
from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.converter_exc_composer import ConversionErrHandlingContext
from bi_external_api.converter.dash import DashboardConverter
from bi_external_api.converter.main import DatasetConverter
from bi_external_api.converter.utils import convert_int_name_remap_to_ext_name_map
from bi_external_api.converter.worbook_list.us_wb_list_gather import USWorkbookListGatherer
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.converter.workbook_ctx_loader import WorkbookContextLoader
from bi_external_api.domain import external as ext
from bi_external_api.domain.external import rpc, ConnectionModifyRequest, ConnectionGetRequest, ConnectionDeleteRequest
from bi_external_api.domain.internal.dl_common import EntryScope
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.workbook_ops import wb_mod_steps
from bi_external_api.workbook_ops.conn_manager import ConnectionManager, ConnectionSecretsHolder, int_conn_data_to_ext
from bi_external_api.workbook_ops.exc_composer import (
    WorkbookOperationErrorHandler,
    WorkbookReadExcComposer,
    WorkbookModificationExcComposer,
    WorkbookClusterizeExcComposer,
)
from bi_external_api.workbook_ops.private_exceptions import (
    WorkbookReadPrivateError,
    OperationTerminationError,
    OperationTerminationErrorData,
)
from bi_external_api.workbook_ops.wb_accessor import WorkbookAccessor
from bi_external_api.workbook_ops.wb_diff_calculator import ExtWorkbookDiffCalculator
from bi_external_api.workbook_ops.wb_mod_steps.utils import TaggedStringAttrSetter
from bi_external_api.workbook_ops.wb_modification_context import WorkbookModificationContext


@attr.s()
class WorkbookOpsFacade:
    _workbook_ctx_loader: WorkbookContextLoader = attr.ib()
    _internal_api_clients: InternalAPIClients = attr.ib()
    _api_type: ExtAPIType = attr.ib()
    _do_add_exc_message: bool = attr.ib(default=True)

    @contextmanager
    def wb_op_err_handler(self, message: str) -> Iterator[None]:
        with WorkbookOperationErrorHandler(
            message=message,
            do_add_exc_message=self._do_add_exc_message,
        ).err_handler() as handler:
            yield handler

    @classmethod
    def convert_wb_context_to_ext_wb_instance(
            cls,
            wb_ctx: WorkbookContext,
            converter_ctx: ConverterContext,
            exc_redux: WorkbookReadExcComposer,
    ) -> ext.WorkBook:
        assert exc_redux.is_opened

        conv_ds = DatasetConverter(wb_ctx, converter_ctx)
        conv_charts = BaseChartConverter(wb_ctx, converter_ctx)
        dash_converter = DashboardConverter(wb_ctx)

        dataset_instances: list[ext.DatasetInstance] = []
        charts_instances: list[ext.ChartInstance] = []
        dash_instances: list[ext.DashInstance] = []

        for ds in wb_ctx.datasets:
            with exc_redux.postponed_error(exc_redux.create_ctx(entry_name=ds.summary.name)):
                dataset_instances.append(ext.DatasetInstance(
                    dataset=conv_ds.convert_internal_dataset_to_public_dataset(ds.dataset),
                    name=ds.summary.name
                ))

        for chart_inst in wb_ctx.charts:
            with exc_redux.postponed_error(exc_redux.create_ctx(entry_name=chart_inst.summary.name)):
                # Error handling context was externalized
                #  to store partially converted charts (like "unsupported visualization").
                # With context inside .convert_chart_int_to_ext() such charts was not returned
                #  by method. Exception with postponed error raised inside.
                with ConversionErrHandlingContext().cm() as conversion_err_handling_ctx:
                    charts_instances.append(ext.ChartInstance(
                        chart=conv_charts.convert_chart_int_to_ext(chart_inst.chart, conversion_err_handling_ctx),
                        name=chart_inst.summary.name,
                    ))

        for dash_inst in wb_ctx.dashboards:
            with exc_redux.postponed_error(exc_redux.create_ctx(entry_name=dash_inst.summary.name)):
                dash_instances.append(ext.DashInstance(
                    dashboard=dash_converter.convert_int_to_ext(dash_inst.dash),
                    name=dash_inst.summary.name,
                ))

        return ext.WorkBook(
            datasets=sorted(dataset_instances, key=lambda ds_inst: ds_inst.name),
            charts=sorted(charts_instances, key=lambda chart_inst: chart_inst.name),
            dashboards=sorted(dash_instances, key=lambda dash_inst: dash_inst.name),
        )

    async def _load_and_convert_workbook(
            self,
            converter_ctx: ConverterContext,
            wb_id: str,
    ) -> tuple[WorkbookContext, ext.WorkBook]:
        with WorkbookReadExcComposer(do_add_exc_message=self._do_add_exc_message) as exc_composer:
            wb_ctx = await self._workbook_ctx_loader.load(wb_id)
            exc_composer.set_initial_wb_context(wb_ctx)

            wb = self.convert_wb_context_to_ext_wb_instance(wb_ctx, converter_ctx, exc_composer)
            exc_composer.set_workbook_partial(wb)

            return wb_ctx, wb

    async def _clusterize_and_convert_workbook(
            self,
            converter_ctx: ConverterContext,
            dash_id_list: Sequence[str],
            navigation_path: Optional[str]
    ) -> tuple[WorkbookContext, ext.WorkBook, Sequence[ext.NameMapEntry]]:
        with WorkbookClusterizeExcComposer(
                do_add_exc_message=self._do_add_exc_message) as exc_composer:
            wb_ctx, name_remap_info = await self._workbook_ctx_loader.gather_workbook_by_dash(
                dash_id_list=dash_id_list,
                us_folder_path=navigation_path,
            )
            exc_composer.set_initial_wb_context(wb_ctx)

            ext_name_map = convert_int_name_remap_to_ext_name_map(name_remap_info)
            exc_composer.set_post_clusterization_name_map(ext_name_map)

            wb = self.convert_wb_context_to_ext_wb_instance(wb_ctx, converter_ctx, exc_composer)
            exc_composer.set_workbook_partial(wb)

            return wb_ctx, wb, ext_name_map

    @classmethod
    def get_broken_entries_map(
            cls,
            wb_ctx: WorkbookContext,
            converted_ext_wb: ext.WorkBook
    ) -> dict[Type[ext.EntryInstance], set[str]]:
        all_converted_entry_names = WorkbookAccessor(converted_ext_wb).all_names

        return {
            inst_clz: {
                summary.name
                for summary in wb_ctx.get_all_summaries()
                if summary.scope == scope and summary.name not in all_converted_entry_names
            }
            for scope, inst_clz in {
                EntryScope.dataset: ext.DatasetInstance,
                EntryScope.widget: ext.ChartInstance,
                EntryScope.dash: ext.DashInstance,
            }.items()
        }

    async def ensure_workbook_config(
            self,
            wb_id: str,
            desired_workbook: ext.WorkBook,
            converter_ctx: ConverterContext,
            force_rewrite: bool = False,
    ) -> WorkbookModificationContext:

        with WorkbookModificationExcComposer(
                do_add_exc_message=self._do_add_exc_message) as exc_composer:
            return await self._ensure_workbook_config_internal(
                wb_id=wb_id,
                desired_workbook=desired_workbook,
                force_rewrite=force_rewrite,
                converter_ctx=converter_ctx,
                exc_composer=exc_composer,
            )

    async def _ensure_workbook_config_internal(
            self,
            wb_id: str,
            desired_workbook: ext.WorkBook,
            exc_composer: WorkbookModificationExcComposer,
            converter_ctx: ConverterContext,
            force_rewrite: bool = False,
    ) -> WorkbookModificationContext:
        initial_wb_ctx: WorkbookContext
        initial_wb: ext.WorkBook

        try:
            initial_wb_ctx, initial_wb = await self._load_and_convert_workbook(converter_ctx, wb_id)
        except WorkbookReadPrivateError as exc:
            if force_rewrite:
                partial_initial_wb = exc.data.partial_workbook
                partial_wb_ctx = exc.data.wb_ctx

                if partial_initial_wb is not None and partial_wb_ctx is not None:
                    initial_wb_ctx = partial_wb_ctx
                    initial_wb = partial_initial_wb
                else:
                    raise exc
            else:
                raise exc

        conv_ds = DatasetConverter(initial_wb_ctx, converter_ctx)
        normalized_desired_workbook = attr.evolve(
            desired_workbook,
            datasets=[
                attr.evolve(ds, dataset=conv_ds.fill_defaults(ds.dataset))
                for ds in desired_workbook.datasets
            ]
        )

        wb_diff_calculator = ExtWorkbookDiffCalculator(
            initial_wb,
            do_force_rewrite=force_rewrite,
            map_inst_clz_broken_entry_name_set=self.get_broken_entries_map(initial_wb_ctx, initial_wb)
        )
        plan = wb_diff_calculator.get_workbook_transition_plan(normalized_desired_workbook)

        wb_modification_ctx_working_copy = WorkbookModificationContext.create(
            wb_id=wb_id,
            plan=plan,
            initial_wb_context=initial_wb_ctx,
        )

        wb_modification_ctx_working_copy = await wb_mod_steps.StepApplyPlanInMemoryDatasets(
            internal_api_clients=self._internal_api_clients,
            wbm_ctx=wb_modification_ctx_working_copy,
            exc_composer=exc_composer,
            converter_ctx=converter_ctx,
        ).execute()
        wb_modification_ctx_working_copy = await wb_mod_steps.StepApplyPlanInMemoryCharts(
            internal_api_clients=self._internal_api_clients,
            wbm_ctx=wb_modification_ctx_working_copy,
            exc_composer=exc_composer,
            converter_ctx=converter_ctx,
        ).execute()
        wb_modification_ctx_working_copy = await wb_mod_steps.StepApplyPlanInMemoryDashboards(
            internal_api_clients=self._internal_api_clients,
            wbm_ctx=wb_modification_ctx_working_copy,
            exc_composer=exc_composer,
            converter_ctx=converter_ctx,
        ).execute()

        if exc_composer.is_armed():
            raise OperationTerminationError(
                OperationTerminationErrorData("Workbook validation failed. Persisting will not be performed."))

        # Persisting
        wb_modification_ctx_working_copy = await wb_mod_steps.StepPersistDatasets(
            internal_api_clients=self._internal_api_clients,
            wbm_ctx=wb_modification_ctx_working_copy,
            exc_composer=exc_composer,
            converter_ctx=converter_ctx,
        ).execute()
        wb_modification_ctx_working_copy = await wb_mod_steps.StepPersistCharts(
            internal_api_clients=self._internal_api_clients,
            wbm_ctx=wb_modification_ctx_working_copy,
            exc_composer=exc_composer,
            converter_ctx=converter_ctx,
        ).execute()
        wb_modification_ctx_working_copy = await wb_mod_steps.StepPersistDashboards(
            internal_api_clients=self._internal_api_clients,
            wbm_ctx=wb_modification_ctx_working_copy,
            exc_composer=exc_composer,
            converter_ctx=converter_ctx,
        ).execute()

        return wb_modification_ctx_working_copy

    #
    # Entry points
    #

    async def create_fake_workbook(self, rq: rpc.FakeWorkbookCreateRequest) -> rpc.FakeWorkbookCreateResponse:
        with WorkbookOperationErrorHandler(message="Workbook creation error").err_handler():
            await self._internal_api_clients.us.create_folder(
                rq.workbook_id
            )
            desired_wb = rq.workbook
            created_entries_info: list[ext.EntryInfo] = []

            if desired_wb is not None:
                conn_manager = ConnectionManager(
                    self._internal_api_clients,
                    ConnectionSecretsHolder(rq.connection_secrets),
                )

                for conn_inst in desired_wb.connections:
                    conn_manager.validate_conn(conn_inst)

                for conn_inst in desired_wb.connections:
                    conn_info = await conn_manager.create_connection(
                        wb_id=rq.workbook_id,
                        conn_inst=conn_inst,
                    )
                    created_entries_info.append(conn_info)

            return rpc.FakeWorkbookCreateResponse(
                workbook_id=rq.workbook_id,
                created_entries_info=created_entries_info,
            )

    async def create_dc_workbook(self, rq: rpc.TrueWorkbookCreateRequest) -> rpc.TrueWorkbookCreateResponse:
        with WorkbookOperationErrorHandler(message="Workbook creation error").err_handler():
            wb_id = await self._internal_api_clients.us.create_workbook(rq.workbook_title)
            return rpc.TrueWorkbookCreateResponse(
                workbook_id=wb_id,
            )

    async def create_connection(self, rq: rpc.ConnectionCreateRequest) -> rpc.ConnectionCreateResponse:
        with WorkbookOperationErrorHandler(message="Connection creation error").err_handler():
            secret = rq.secret
            conn_sec_holder: ConnectionSecretsHolder

            if secret is None:
                conn_sec_holder = ConnectionSecretsHolder([])
            else:
                conn_sec_holder = ConnectionSecretsHolder([
                    ext.ConnectionSecret(secret=secret, conn_name=rq.connection.name)
                ])

            conn_manager = ConnectionManager(self._internal_api_clients, conn_sec_holder)
            conn_manager.validate_conn(rq.connection)
            conn_info = await conn_manager.create_connection(rq.connection, wb_id=rq.workbook_id)

            return rpc.ConnectionCreateResponse(
                connection_info=conn_info,
            )

    async def modify_connection(self, rq: ConnectionModifyRequest) -> rpc.ConnectionModifyResponse:
        with WorkbookOperationErrorHandler(message="Connection modification error").err_handler():

            wb_ctx = await self._workbook_ctx_loader.load(rq.workbook_id, connections_only=True)
            conn_ref = wb_ctx.resolve_connection_by_name(rq.connection.name)

            secret = rq.secret
            conn_sec_holder: ConnectionSecretsHolder

            if secret is None:
                conn_sec_holder = ConnectionSecretsHolder([])
            else:
                conn_sec_holder = ConnectionSecretsHolder(
                    [ext.ConnectionSecret(secret=secret, conn_name=rq.connection.name)])

            conn_manager = ConnectionManager(self._internal_api_clients, conn_sec_holder)
            conn_manager.validate_conn(rq.connection)
            await conn_manager.modify_connection(
                rq.connection,
                wb_id=rq.workbook_id,
                conn_id=conn_ref.id,
            )

            return rpc.ConnectionModifyResponse()

    async def get_connection(self, rq: ConnectionGetRequest) -> rpc.ConnectionGetResponse:

        with WorkbookOperationErrorHandler(message="Connection get error").err_handler():
            wb_ctx = await self._workbook_ctx_loader.load(rq.workbook_id, connections_only=True)
            conn_ref = wb_ctx.resolve_connection_by_name(rq.name)

            bi_conn, data = await self._internal_api_clients.datasets_cp.get_connection_with_data(
                conn_id=conn_ref.id,
            )
            conn_info = ext.EntryInfo(kind=ext.EntryKind.connection, name=conn_ref.name, id=bi_conn.id)

            _kind_to_model = {
                CONNECTION_TYPE_CLICKHOUSE: ext.ClickHouseConnection,
            }

            _type = _kind_to_model[bi_conn.type]

            conn = int_conn_data_to_ext(self._api_type, _type, data)

            resp = rpc.ConnectionGetResponse(
                connection=ext.ConnectionInstance(
                    connection=conn,
                    name=conn_info.name,
                )
            )
            return resp

    async def delete_connection(self, rq: ConnectionDeleteRequest) -> rpc.ConnectionDeleteResponse:
        with WorkbookOperationErrorHandler(message="Connection deletion error").err_handler():
            wb_ctx = await self._workbook_ctx_loader.load(rq.workbook_id, connections_only=True)
            conn_ref = wb_ctx.resolve_connection_by_name(rq.name)
            await self._internal_api_clients.datasets_cp.delete_connection(conn_ref.id)
            return rpc.ConnectionDeleteResponse()

    async def write_workbook(self, rq: rpc.WorkbookWriteRequest) -> rpc.WorkbookWriteResponse:
        with WorkbookOperationErrorHandler(message="Workbook writing error").err_handler():
            final_wbm = await self.ensure_workbook_config(
                wb_id=rq.workbook_id,
                desired_workbook=rq.workbook,
                converter_ctx=ConverterContext(),
                force_rewrite=bool(rq.force_rewrite),
            )
            return rpc.WorkbookWriteResponse(
                workbook=rq.workbook,
                executed_plan=final_wbm.transition_plan.to_external(),
            )

    async def read_workbook(self, rq: rpc.WorkbookReadRequest) -> rpc.WorkbookReadResponse:
        with self.wb_op_err_handler(message="Workbook reading error"):
            wb_ctx, ext_workbook = await self._load_and_convert_workbook(
                converter_ctx=ConverterContext(use_id_formula=rq.use_id_formula),
                wb_id=rq.workbook_id,
            )
        if wb_ctx.wb_basic_info is not None:
            return rpc.WorkbookReadResponse(
                workbook=ext_workbook,
                id=wb_ctx.wb_basic_info.id,
                title=wb_ctx.wb_basic_info.title,
                project_id=wb_ctx.wb_basic_info.project_id
            )
        else:
            return rpc.WorkbookReadResponse(
                workbook=ext_workbook
            )

    async def delete_workbook(self, rq: rpc.WorkbookDeleteRequest) -> rpc.WorkbookDeleteResponse:
        with self.wb_op_err_handler(message="Workbook deletion error"):
            await self._internal_api_clients.us.delete_workbook(rq.workbook_id)

        return rpc.WorkbookDeleteResponse()

    async def clusterize_workbook(self, rq: rpc.WorkbookClusterizeRequest) -> rpc.WorkbookClusterizeResponse:
        with self.wb_op_err_handler(message="Workbook clusterization error"):
            _, ext_workbook, name_map = await self._clusterize_and_convert_workbook(
                converter_ctx=ConverterContext(),
                dash_id_list=rq.dash_id_list,
                navigation_path=rq.navigation_folder_path
            )

        return rpc.WorkbookClusterizeResponse(
            workbook=ext_workbook,
            name_map=name_map,
        )

    async def advise_dataset_fields(
            self,
            rq: rpc.AdviseDatasetFieldsRequest,
    ) -> rpc.AdviseDatasetFieldsResponse:
        with self.wb_op_err_handler(message="Fields generation error"):
            target_conn_ref = rq.connection_ref

            wb_ctx: WorkbookContext
            real_connection_name: str

            if isinstance(target_conn_ref, ext.EntryIDRef):
                wb_ctx = await self._workbook_ctx_loader.load_for_single_connection(conn_id=target_conn_ref.id)
                real_connection_name = next(iter(wb_ctx.connections)).summary.name

            elif isinstance(target_conn_ref, ext.EntryWBRef):
                wb_ctx = await self._workbook_ctx_loader.load(target_conn_ref.wb_id, connections_only=True)
                real_connection_name = target_conn_ref.entry_name

            else:
                raise AssertionError(
                    f"Got unexpected type of connection ref in AdviseDatasetFieldsRequest: {type(target_conn_ref)}"
                )

            converter_ctx = ConverterContext(
                use_id_formula=rq.use_id_formula or False,
            )

            converter = DatasetConverter(wb_ctx, converter_ctx=converter_ctx)
            actions = converter.convert_public_dataset_to_actions(
                dataset=converter.fill_defaults(
                    TaggedStringAttrSetter(
                        tag=ext.ExtModelTags.connection_name,
                        value_to_set=real_connection_name
                    ).process(rq.partial_dataset)
                ),
                generate_direct_fields=True
            )

            dataset, _ = await self._internal_api_clients.datasets_cp.build_dataset_config_by_actions(actions)

            ext_dataset = converter.convert_internal_dataset_to_public_dataset(dataset)
            # Cut random suffixes to generate stable IDs
            ext_dataset = attr.evolve(
                ext_dataset,
                fields=[
                    attr.evolve(
                        f,
                        # Cutting off IDs suffixes to make results stable
                        id=re.sub(r"_\w{4}$", "", f.id)
                    )
                    for f in ext_dataset.fields
                ]
            )

            return rpc.AdviseDatasetFieldsResponse(
                dataset=ext_dataset
            )

    async def list_workbooks(self, rq: rpc.WorkbookListRequest) -> rpc.WorkbookListResponse:
        with self.wb_op_err_handler(message="Workbook list"):
            workbooks = await USWorkbookListGatherer(
                us_client=self._internal_api_clients.us
            ).gather_workbooks()

            return rpc.WorkbookListResponse(workbooks=workbooks)

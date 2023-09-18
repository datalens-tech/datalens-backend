import asyncio
from collections import deque
from typing import (
    Awaitable,
    Deque,
    Iterable,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

import attr

from bi_external_api.converter import converter_exc
from bi_external_api.converter.workbook import (
    EntryLoadFailInfo,
    WorkbookContext,
)
from bi_external_api.converter.workbook_gathering_ctx import (
    IDNameNormalizer,
    NameNormalizer,
    WorkbookGatheringContext,
)
from bi_external_api.domain.internal import charts as chart_models
from bi_external_api.domain.internal import dashboards as dash_models
from bi_external_api.domain.internal import datasets as dataset_models
from bi_external_api.domain.internal.dl_common import (
    EntryInstance,
    EntryScope,
    EntrySummary,
)
from bi_external_api.internal_api_clients.main import (
    InstanceLoadInfo,
    InternalAPIClients,
)
from bi_external_api.internal_api_clients.models import (
    WorkbookBackendTOC,
    WorkbookBasicInfo,
)

_INST_TYPE_TV = TypeVar("_INST_TYPE_TV", bound=EntryInstance)


@attr.s(frozen=True)
class WorkbookContextLoader:
    _internal_api_clients: InternalAPIClients = attr.ib()
    _use_workbooks_api: bool = attr.ib(default=False)

    async def _load_entries_by_summaries(
        self, clz: Type[_INST_TYPE_TV], entry_summary_iterable: Iterable[EntrySummary]
    ) -> tuple[Sequence[_INST_TYPE_TV], Iterable[EntryLoadFailInfo]]:
        map_entry_id_to_summary: dict[str, EntrySummary] = {summary.id: summary for summary in entry_summary_iterable}
        loaded_instances, map_id_exc = await self._load_entries_by_ids(clz, map_entry_id_to_summary.keys())
        return loaded_instances, [
            EntryLoadFailInfo(summary=map_entry_id_to_summary[entry_id], exception=exc)
            for entry_id, exc in map_id_exc.items()
        ]

    async def _load_entries_by_ids(
        self, clz: Type[_INST_TYPE_TV], id_iterable: Iterable[str]
    ) -> tuple[Sequence[_INST_TYPE_TV], dict[str, Exception]]:
        entry_load_task_list: list[Awaitable[InstanceLoadInfo[_INST_TYPE_TV]]] = []

        for entry_id in id_iterable:
            entry_load_task_list.append(
                asyncio.create_task(self._internal_api_clients.get_instance_by_id(clz, entry_id))
            )

        load_info_list: Sequence[InstanceLoadInfo[_INST_TYPE_TV]] = await asyncio.gather(*entry_load_task_list)
        entry_load_fail_info_list = {li.requested_entry_id: li.exc for li in load_info_list if not li.is_ok()}
        loaded_instances = [li.instance for li in load_info_list if li.is_ok()]

        return loaded_instances, entry_load_fail_info_list

    async def load_for_single_connection(self, conn_id: str) -> WorkbookContext:
        conn_inst = await self._internal_api_clients.datasets_cp.get_connection(conn_id)
        return WorkbookContext(
            connections=(conn_inst,),
            datasets=(),
            charts=(),
            dashboards=(),
            load_fail_info_collection=(),
        )

    async def load(self, wb_id: str, *, connections_only: Optional[bool] = False) -> WorkbookContext:
        wb_backend_toc: WorkbookBackendTOC
        wb_basic_info: Optional[WorkbookBasicInfo]

        if self._use_workbooks_api:
            wb_backend_toc = await self._internal_api_clients.us.get_workbook_backend_toc(wb_id)
            wb_basic_info = await self._internal_api_clients.us.get_workbook_basic_info(wb_id)
        else:
            wb_backend_toc = await self._internal_api_clients.datasets_cp.get_workbook_backend_toc(wb_id)
            wb_basic_info = None

        entry_load_fail_info_list: list[EntryLoadFailInfo] = []
        all_charts: Sequence[chart_models.ChartInstance] = []
        all_dashboards: Sequence[dash_models.DashInstance] = []
        all_datasets: Sequence[dataset_models.DatasetInstance] = []

        if not connections_only:
            all_datasets, broken_ds_summaries = await self._load_entries_by_summaries(
                dataset_models.DatasetInstance, wb_backend_toc.datasets
            )
            entry_load_fail_info_list.extend(broken_ds_summaries)

            if self._internal_api_clients.charts is not None:
                all_charts, broken_chart_summaries = await self._load_entries_by_summaries(
                    chart_models.ChartInstance,
                    wb_backend_toc.charts,
                )
                entry_load_fail_info_list.extend(broken_chart_summaries)

            if self._internal_api_clients.dash is not None:
                all_dashboards, broken_dash_summaries = await self._load_entries_by_summaries(
                    dash_models.DashInstance,
                    wb_backend_toc.dashboards,
                )
                entry_load_fail_info_list.extend(broken_dash_summaries)

        return WorkbookContext(
            connections=[
                dataset_models.ConnectionInstance.create_from_bi_connection_summary(conn_summary)
                for conn_summary in wb_backend_toc.connections
            ],
            datasets=all_datasets,
            charts=all_charts,
            dashboards=all_dashboards,
            load_fail_info_collection=entry_load_fail_info_list,
            wb_basic_info=wb_basic_info,
        )

    async def gather_dash_summaries(self, us_path: str) -> Sequence[EntrySummary]:
        max_folders = 100
        us_cli = self._internal_api_clients.us

        traversed_folders: list[str] = []
        folders_to_traverse: Deque[str] = deque([us_path])
        dash_summaries: list[EntrySummary] = []

        while folders_to_traverse:
            path = folders_to_traverse.popleft()
            path_summaries = await us_cli.get_folder_content(path)
            traversed_folders.append(path)

            next_folders_to_traverse = [summary for summary in path_summaries if summary.scope is EntryScope.dash]

            if len(traversed_folders) + len(next_folders_to_traverse) >= max_folders:
                raise converter_exc.LimitExhausted(f"Folder traversing limit exhausted: {max_folders}")

            dash_summaries.extend(next_folders_to_traverse)

            folders_to_traverse.extend(
                [
                    f"{summary.workbook_id}/{summary.name}"
                    for summary in path_summaries
                    if summary.scope is EntryScope.folder
                ]
            )

        return dash_summaries

    async def gather_workbook_by_dash(
        self,
        dash_id_list: Optional[Sequence[str]] = None,
        us_folder_path: Optional[str] = None,
        name_normalizer_cls: Optional[Type[NameNormalizer]] = None,
    ) -> tuple[WorkbookContext, dict[str, EntrySummary]]:
        wb_id = "ephemeral"
        dash_ids_to_load: list[str] = []

        if dash_id_list is not None:
            dash_ids_to_load.extend(dash_id_list)

        if us_folder_path is not None:
            extracted_from_us_folder_dash_summaries = await self.gather_dash_summaries(us_folder_path)
            dash_ids_to_load.extend([summary.id for summary in extracted_from_us_folder_dash_summaries])

        wb_gathering_ctx = WorkbookGatheringContext(
            wb_id=wb_id,
            initial_dash_ids_to_load=dash_ids_to_load,
            name_normalizer_cls=name_normalizer_cls if name_normalizer_cls is not None else IDNameNormalizer,
        )

        instance_type_loading_order: Sequence[Type[EntryInstance]] = (
            dash_models.DashInstance,
            chart_models.ChartInstance,
            dataset_models.DatasetInstance,
            dataset_models.ConnectionInstance,
        )

        for clz in instance_type_loading_order:
            ids_to_load = wb_gathering_ctx.get_ids_to_load(clz)
            loaded_instances, map_entry_id_to_load_exc = await self._load_entries_by_ids(clz, ids_to_load)

            wb_gathering_ctx.add_loaded_instances(loaded_instances)
            wb_gathering_ctx.add_load_fail_info(clz, map_entry_id_to_load_exc)

        return wb_gathering_ctx.build_workbook_context()

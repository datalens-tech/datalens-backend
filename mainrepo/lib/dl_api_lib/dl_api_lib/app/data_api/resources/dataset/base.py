from __future__ import annotations

from contextlib import (
    AsyncExitStack,
    asynccontextmanager,
)
import logging
import os
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    ClassVar,
    Collection,
    Dict,
    List,
    Optional,
    Type,
)

from aiohttp import web

from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon
from dl_api_lib import utils
from dl_api_lib.api_common.data_serialization import (
    DataRequestResponseSerializer,
    get_fields_data_serializable,
)
from dl_api_lib.api_common.dataset_loader import (
    DatasetApiLoader,
    DatasetUpdateInfo,
)
from dl_api_lib.api_common.update_dataset_mutation_key import UpdateDatasetMutationKey
from dl_api_lib.app.data_api.resources.base import (
    BaseView,
    requires,
)
from dl_api_lib.dataset.view import DatasetView
from dl_api_lib.query.formalization.block_formalizer import BlockFormalizer
from dl_api_lib.query.formalization.legend_formalizer import (
    DistinctLegendFormalizer,
    LegendFormalizer,
    PivotLegendFormalizer,
    PreviewLegendFormalizer,
    RangeLegendFormalizer,
    ResultLegendFormalizer,
)
from dl_api_lib.request_model.data import (
    Action,
    DataRequestModel,
    FieldAction,
)
from dl_api_lib.service_registry.service_registry import BiApiServiceRegistry
from dl_app_tools.profiling_base import GenericProfiler
from dl_constants.enums import DataSourceRole
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.data_source.base import DataSource
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.dataset_capabilities import DatasetCapabilities
from dl_core.exc import USObjectNotFoundException
from dl_core.us_connection_base import ExecutorBasedMixin
from dl_core.us_dataset import Dataset
from dl_core.us_manager.mutation_cache.engine_factory import CacheInitializationError
from dl_core.us_manager.mutation_cache.mutation_key_base import MutationKey
from dl_core.us_manager.mutation_cache.usentry_mutation_cache import (
    RedisCacheEngine,
    USEntryMutationCache,
)
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_query_processing.enums import QueryType
from dl_query_processing.execution.exec_info import QueryExecutionInfo
from dl_query_processing.legend.block_legend import BlockSpec
from dl_query_processing.merging.merger import DataStreamMerger
from dl_query_processing.merging.primitives import MergedQueryDataStream
from dl_query_processing.pagination.paginator import QueryPaginator
from dl_query_processing.postprocessing.postprocessor import DataPostprocessor
from dl_query_processing.postprocessing.primitives import (
    PostprocessedQuery,
    PostprocessedQueryBlock,
    PostprocessedQueryUnion,
    PostprocessedRow,
)
from dl_utils.task_runner import ConcurrentTaskRunner

if TYPE_CHECKING:
    from dl_api_lib.query.formalization.raw_specs import RawQuerySpecUnion


LOGGER = logging.getLogger(__name__)


@requires(RequiredResourceCommon.US_MANAGER)
class DatasetDataBaseView(BaseView):
    STORED_DATASET_REQUIRED: ClassVar[bool] = True

    profiler_prefix: ClassVar[str]

    dataset: Dataset
    ds_accessor: DatasetComponentAccessor

    @property
    def dataset_id(self) -> Optional[str]:
        return self.request.match_info.get("ds_id")

    @property
    def allow_query_cache_usage(self) -> bool:
        """
        Indicates if query cache may be used for request.
         Now only config flag but in future - may be special header.
        """
        return self.dl_request.app_wrapper.allow_query_cache_usage

    @property
    def allow_notifications(self) -> bool:
        """
        Indicates if may be returned in response.
        Now only config flag but in future - may be more complex logic
        """
        return self.dl_request.app_wrapper.allow_notifications

    @asynccontextmanager  # type: ignore  # TODO: fix
    async def default_query_execution_cm_stack(
        self,
        exec_info: QueryExecutionInfo,
        body: dict,
        profiling_code: str = "query-execute",
    ) -> AsyncIterable[AsyncExitStack]:
        """
        See also:
        `dl_api_lib.app.data_api.resources.dashsql.DashSQLView.enrich_logging_context`
        """
        async with AsyncExitStack() as stack:
            if self.dl_request.log_ctx_controller:
                try:
                    target_connections = exec_info.target_connections
                    assert len(target_connections) == 1
                    target_conn = target_connections[0]
                    self.dl_request.log_ctx_controller.put_to_context("conn_id", target_conn.uuid)
                    self.dl_request.log_ctx_controller.put_to_context("conn_type", target_conn.conn_type.name)

                    sr = self.dl_request.services_registry
                    try:
                        if isinstance(target_conn, ExecutorBasedMixin):
                            ce_cls_str = (
                                sr.get_conn_executor_factory().get_async_conn_executor_cls(target_conn).__qualname__
                            )
                        else:
                            ce_cls_str = "N/D"
                        self.dl_request.log_ctx_controller.put_to_context("conn_exec_cls", ce_cls_str)
                    except Exception:  # noqa
                        LOGGER.exception("Can not get CE class for connection %s", target_conn.uuid)

                except Exception:  # noqa
                    LOGGER.exception("Can not save connection info to logging context")

            stack.enter_context(GenericProfiler(f"{self.profiler_prefix}-{profiling_code}"))  # type: ignore  # TODO: fix
            stack.enter_context(utils.query_execution_context(dataset_id=self.dataset.uuid, version="draft", body=body))
            yield stack

    def resolve_dataset_source_role(self, dataset: Dataset, log_reasons: bool = False) -> DataSourceRole:
        dsrc_coll_factory = DataSourceCollectionFactory(us_entry_buffer=self.dl_request.us_manager.get_entry_buffer())
        capabilities = DatasetCapabilities(dataset=dataset, dsrc_coll_factory=dsrc_coll_factory)
        return capabilities.resolve_source_role(log_reasons=log_reasons)

    async def resolve_entities(self) -> None:
        us_manager = self.dl_request.us_manager
        if self.dl_request.log_ctx_controller:
            self.dl_request.log_ctx_controller.put_to_context("dataset_id", self.dataset_id)

        if self.dataset_id is None:
            if self.STORED_DATASET_REQUIRED:
                raise ValueError(f"View {self} requires stored dataset, but no ID found in match info")

            dataset = Dataset.create_from_dict(
                Dataset.DataModel(name=""),  # TODO: Remove name - it's not used, but is required
                us_manager=us_manager,  # type: ignore  # TODO: fix # WTF??? sync or async??
            )
        else:
            try:
                dataset = await us_manager.get_by_id(self.dataset_id, Dataset)
            except USObjectNotFoundException:
                raise web.HTTPNotFound(reason="Entity not found")

            await us_manager.load_dependencies(dataset)

        self.dataset = dataset
        self.ds_accessor = DatasetComponentAccessor(dataset=dataset)

    @staticmethod
    def _updates_only_fields(updates: List[Action]) -> bool:
        # Checks if updates has only field updates
        return all([isinstance(upd, FieldAction) for upd in updates])

    def try_get_mutation_key(self, updates: List[Action]) -> Optional[MutationKey]:
        if self.dataset_id is not None and self.dataset.revision_id is not None:
            if self._updates_only_fields(updates):
                return UpdateDatasetMutationKey.create(self.dataset.revision_id, updates)  # type: ignore
        return None

    def try_get_cache(self) -> Optional[USEntryMutationCache]:
        try:
            mc_factory = self.dl_request.services_registry.get_mutation_cache_factory()
            if mc_factory is None:
                LOGGER.debug("Mutation cache is disabled")
                return None
            mce_factory = self.dl_request.services_registry.get_mutation_cache_engine_factory(RedisCacheEngine)
            cache_engine = mce_factory.get_cache_engine()
            mutation_cache = mc_factory.get_mutation_cache(
                usm=self.dl_request.us_manager,
                engine=cache_engine,
            )
            return mutation_cache
        except CacheInitializationError:  # Error creating factory with redis cache engine or something
            LOGGER.error("Mutation cache error", exc_info=True)
            return None

    async def try_get_dataset_from_cache(
        self,
        mutation_cache: Optional[USEntryMutationCache],
        mutation_key: Optional[MutationKey],
    ) -> Optional[Dataset]:
        if mutation_key is None or mutation_cache is None:
            return None
        if self.dataset_id is None or self.dataset.revision_id is None:
            return None

        cached_dataset = await mutation_cache.get_mutated_entry_from_cache(
            Dataset,
            self.dataset_id,
            self.dataset.revision_id,
            mutation_key,
        )
        if cached_dataset is None:
            return None
        LOGGER.info("Found dataset in mutation cache")
        cached_dataset.permissions_mode = self.dataset.permissions_mode
        cached_dataset.permissions = self.dataset.permissions
        assert isinstance(cached_dataset, Dataset)
        return cached_dataset

    async def try_save_dataset_to_cache(
        self,
        mutation_cache: Optional[USEntryMutationCache],
        mutation_key: Optional[MutationKey],
        dataset: Dataset,
    ) -> None:
        if mutation_key is None:
            return None
        if mutation_cache is None:
            return None
        await mutation_cache.save_mutation_cache(dataset, mutation_key)

    async def prepare_dataset_for_request(
        self,
        req_model: DataRequestModel,
        allow_rls_change: bool = False,
        enable_mutation_caching: bool = False,
    ) -> DatasetUpdateInfo:
        us_manager = self.dl_request.us_manager
        services_registry = self.dl_request.services_registry
        assert isinstance(services_registry, BiApiServiceRegistry)
        loader = DatasetApiLoader(service_registry=services_registry)

        with GenericProfiler("dataset-prepare"):
            if enable_mutation_caching:
                mutation_cache = self.try_get_cache()
                mutation_key = self.try_get_mutation_key(req_model.updates)
                cached_dataset = await self.try_get_dataset_from_cache(mutation_cache, mutation_key)
                if cached_dataset:
                    self.dataset = cached_dataset
                    return loader.update_dataset_from_body(
                        dataset=self.dataset,
                        us_manager=us_manager,
                        dataset_data=req_model.dataset,
                        allow_rls_change=allow_rls_change,
                    )

            update_info = loader.update_dataset_from_body(
                dataset=self.dataset,
                us_manager=us_manager,
                dataset_data=req_model.dataset,
                allow_rls_change=allow_rls_change,
            )
            await us_manager.load_dependencies(self.dataset)

            services_registry = self.dl_request.services_registry
            assert isinstance(services_registry, BiApiServiceRegistry)

            await self.check_for_notifications(services_registry, us_manager)

            dataset_validator_factory = services_registry.get_dataset_validator_factory()
            ds_validator = dataset_validator_factory.get_dataset_validator(
                ds=self.dataset, us_manager=us_manager, is_data_api=True
            )
            executor = services_registry.get_compute_executor()
            await executor.execute(lambda: ds_validator.apply_batch(action_batch=req_model.updates))
            if enable_mutation_caching:
                await self.try_save_dataset_to_cache(mutation_cache, mutation_key, self.dataset)  # noqa

        return update_info

    async def check_for_notifications(
        self, services_registry: BiApiServiceRegistry, us_manager: AsyncUSManager
    ) -> None:
        ds_lc_manager = us_manager.get_lifecycle_manager(self.dataset)
        for conn_id in ds_lc_manager.collect_links().values():
            try:
                conn = us_manager.get_loaded_us_connection(conn_id)
            except Exception:
                LOGGER.info("Failed to get loaded us connection %s", conn_id, exc_info=True)
            else:
                conn_notifications = conn.check_for_notifications()
                if not conn_notifications:
                    continue

                reporting_registry = services_registry.get_reporting_registry()
                for notification_record in conn_notifications:
                    reporting_registry.save_reporting_record(notification_record)

    def make_legend_formalizer(self, query_type: QueryType, autofill_legend: bool = False) -> LegendFormalizer:
        legend_formalizer_cls: Type[LegendFormalizer]
        if query_type == QueryType.pivot:
            legend_formalizer_cls = PivotLegendFormalizer
        elif query_type == QueryType.result:
            legend_formalizer_cls = ResultLegendFormalizer
        elif query_type == QueryType.preview:
            legend_formalizer_cls = PreviewLegendFormalizer
        elif query_type == QueryType.distinct:
            legend_formalizer_cls = DistinctLegendFormalizer
        elif query_type == QueryType.value_range:
            legend_formalizer_cls = RangeLegendFormalizer
        else:
            raise ValueError(f"Legend formalization is not supported for {query_type.name} query type")
        return legend_formalizer_cls(
            dataset=self.dataset,
            autofill_legend=autofill_legend,
        )

    def make_block_formalizer(self) -> BlockFormalizer:
        return BlockFormalizer(
            dataset=self.dataset,
            reporting_registry=self.dl_request.services_registry.get_reporting_registry(),
        )

    async def _call_post_exec_async_hook(self, target_connection_ids: set[str]) -> None:
        for connection_id in target_connection_ids:
            connection = self.dl_request.us_manager.get_loaded_us_connection(connection_id)
            lifecycle_manager = self.dl_request.us_manager.get_lifecycle_manager(
                entry=connection,
                service_registry=self.dl_request.services_registry,
            )
            await lifecycle_manager.post_exec_async_hook()

    async def execute_query(
        self,
        block_spec: BlockSpec,
        possible_data_lengths: Optional[Collection] = None,
        profiling_postfix: str = "",
    ) -> PostprocessedQuery:
        # TODO: Move to a separate class

        us_manager = self.dl_request.us_manager

        ds_view = DatasetView(
            ds=self.dataset,
            us_manager=us_manager,
            block_spec=block_spec,
            rci=self.dl_request.rci,
        )

        with GenericProfiler(f"{self.profiler_prefix}-query-build{profiling_postfix}"):
            exec_info = ds_view.build_exec_info()

        async with self.default_query_execution_cm_stack(exec_info, body=self.dl_request.json):
            executed_query = await ds_view.get_data_async(
                exec_info=exec_info,
                allow_cache_usage=self.allow_query_cache_usage,
            )
            if possible_data_lengths is not None:
                assert len(executed_query.rows) in possible_data_lengths

        postprocessor = DataPostprocessor(profiler_prefix=self.profiler_prefix)
        postprocessed_query = postprocessor.get_postprocessed_data(
            executed_query=executed_query,
            block_spec=block_spec,
        )

        return postprocessed_query

    async def execute_all_queries(
        self,
        raw_query_spec_union: RawQuerySpecUnion,
        autofill_legend: bool,
        call_post_exec_async_hook: bool = False,
    ) -> MergedQueryDataStream:
        # TODO: Move to a separate class

        legend_formalizer = self.make_legend_formalizer(
            query_type=raw_query_spec_union.meta.query_type, autofill_legend=autofill_legend
        )
        legend = legend_formalizer.make_legend(raw_query_spec_union=raw_query_spec_union)

        block_legend = self.make_block_formalizer().make_block_legend(
            raw_query_spec_union=raw_query_spec_union,
            legend=legend,
        )
        paginator = QueryPaginator()
        pre_paginator = paginator.get_pre_paginator()
        post_paginator = paginator.get_post_paginator()
        block_legend = pre_paginator.pre_paginate(block_legend=block_legend)

        concurrency_limit = int(os.environ.get("DATASET_CONCURRENCY_LIMIT", 5))
        runner = ConcurrentTaskRunner(concurrency_limit=concurrency_limit)
        for block_spec in block_legend.blocks:
            await runner.schedule(self.execute_query(block_spec=block_spec))
        executed_queries = await runner.finalize()
        postprocessed_query_blocks = [
            PostprocessedQueryBlock.from_block_spec(block_spec, postprocessed_query=postprocessed_query)
            for block_spec, postprocessed_query in zip(block_legend.blocks, executed_queries)
        ]

        postprocessed_query_union = PostprocessedQueryUnion(
            blocks=postprocessed_query_blocks,
            legend=legend,
            limit=block_legend.meta.limit,
            offset=block_legend.meta.offset,
        )
        merged_stream = DataStreamMerger().merge(postprocessed_query_union=postprocessed_query_union)
        merged_stream = post_paginator.post_paginate(merged_stream=merged_stream)

        if call_post_exec_async_hook:
            await self._call_post_exec_async_hook(merged_stream.meta.target_connection_ids)

        return merged_stream

    def _make_response_v1(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
        totals_query: Optional[str] = None,
        totals: Optional[PostprocessedRow] = None,
    ) -> Dict[str, Any]:
        add_fields_data = req_model.add_fields_data
        fields_data: Optional[List[Dict[str, Any]]] = None
        if add_fields_data:
            fields_data = get_fields_data_serializable(self.dataset, for_result=True)
            LOGGER.info("Field schema data", extra=dict(fields=fields_data))

        response_json = DataRequestResponseSerializer.make_data_response_v1(
            merged_stream=merged_stream,
            totals=totals,
            totals_query=totals_query,
            data_export_forbidden=self.get_data_export_forbidden_flag(),
            fields_data=fields_data,
        )
        return response_json

    def _make_response_v2(self, merged_stream: MergedQueryDataStream) -> Dict[str, Any]:
        result = DataRequestResponseSerializer.make_data_response_v2(
            merged_stream=merged_stream,
            reporting_registry=self.dl_request.services_registry.get_reporting_registry()
            if self.allow_notifications
            else None,
            data_export_forbidden=self.get_data_export_forbidden_flag(),
        )
        return result

    def get_data_export_forbidden_flag(self) -> bool:
        role = self.resolve_dataset_source_role(dataset=self.dataset)
        avatar_ids = [avatar.id for avatar in self.ds_accessor.get_avatar_list()]
        dsrc_coll_factory = DataSourceCollectionFactory(us_entry_buffer=self.dl_request.us_manager.get_entry_buffer())
        for avatar_id in avatar_ids:
            avatar = self.ds_accessor.get_avatar_strict(avatar_id=avatar_id)
            dsrc_coll_spec = self.ds_accessor.get_data_source_coll_spec_strict(source_id=avatar.source_id)
            dsrc_coll = dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)

            dsrc: DataSource
            if DataSourceRole.origin in dsrc_coll:
                dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
            else:
                dsrc = dsrc_coll.get_strict(role)

            if dsrc.data_export_forbidden:
                return True

        return False

    def check_dataset_revision_id(self, req_model: DataRequestModel) -> None:
        dataset_revision_id = req_model.dataset_revision_id
        if dataset_revision_id is not None and dataset_revision_id != self.dataset.revision_id:
            LOGGER.warning(
                f"Dataset revision id mismatch: got {dataset_revision_id} from the request, "
                f"but found {self.dataset.revision_id} in the current dataset"
            )

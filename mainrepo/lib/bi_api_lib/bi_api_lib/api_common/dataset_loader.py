from __future__ import annotations

from copy import deepcopy
import logging
from typing import (
    NamedTuple,
    Optional,
)

import attr

from bi_api_lib import exc
from bi_api_lib.dataset.utils import allow_rls_for_dataset
from bi_api_lib.service_registry.service_registry import BiApiServiceRegistry
from bi_api_lib.utils.rls import FieldRLSSerializer
from bi_app_tools.profiling_base import generic_profiler
from bi_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)
from bi_core.base_models import (
    DefaultConnectionRef,
    DefaultWhereClause,
)
from bi_core.components.accessor import DatasetComponentAccessor
from bi_core.components.editor import DatasetComponentEditor
from bi_core.data_source import get_parameters_hash
from bi_core.data_source.collection import DataSourceCollectionBase
from bi_core.db import are_raw_schemas_same
import bi_core.exc as core_exc
from bi_core.us_dataset import (
    Dataset,
    DataSourceRole,
)
from bi_core.us_manager.local_cache import USEntryBuffer
from bi_core.us_manager.us_manager import USManagerBase
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_utils.aio import await_sync

LOGGER = logging.getLogger(__name__)


class DatasetUpdateInfo(NamedTuple):
    """A simple container that contains info about what has changed in the dataset"""

    added_own_source_ids: list[str]
    updated_own_source_ids: list[str]


EMPTY_DS_UPDATE_INFO = DatasetUpdateInfo(
    added_own_source_ids=[],
    updated_own_source_ids=[],
)


@attr.s
class DatasetApiLoader:
    _service_registry: BiApiServiceRegistry = attr.ib(kw_only=True)

    def update_dataset_from_body(
        self,
        dataset: Dataset,
        us_manager: USManagerBase,
        dataset_data: Optional[dict],
        allow_rls_change: bool = True,
    ) -> DatasetUpdateInfo:
        update_info = EMPTY_DS_UPDATE_INFO

        if dataset_data:
            update_info = self.populate_dataset_from_body(
                dataset=dataset,
                us_manager=us_manager,
                body=dataset_data,
                allow_rls_change=allow_rls_change,
            )
        return update_info

    def _ensure_additional_connections_are_preloaded(
        self,
        dataset_data: Optional[dict],
        us_manager: USManagerBase,
    ) -> None:
        """
        Ensure that the connections used by the dataset are pre-loaded.
        This can be one of 2 cases:
        1. The connection is already a part of the dataset.
           In this case it should have already been loaded with the dataset,
           and nothing will be done.
        2. The connection is not present in the dataset, but is being added
           to it via the patch `dataset_data`.

        Both of these cases will work with sync US manager,
        but the second one will not with an async one.
        On the other hand, this should not be a problem because
        we don't allow modification of data sources in the async server (data-api).
        """

        if dataset_data is None:
            return

        for source_data in dataset_data.get("sources", []):
            if isinstance(us_manager, SyncUSManager):
                # Force pre-loading of the used connection
                connection_id = source_data["connection_id"]
                connection_ref = DefaultConnectionRef(conn_id=connection_id)
                try:
                    us_manager.ensure_entry_preloaded(connection_ref)
                except core_exc.ReferencedUSEntryNotFound:
                    # Ignore deleted connections here - an error will be raised
                    # when it is fetched from the buffer
                    pass
            else:
                pass  # TODO: Maybe validate that the patch doesn't contain any new connections?

    def _update_dataset_sources_from_body(
        self,
        dataset: Dataset,
        body: dict,
        us_entry_buffer: USEntryBuffer,
    ) -> tuple[list, list, set]:
        ds_accessor = DatasetComponentAccessor(dataset=dataset)
        ds_editor = DatasetComponentEditor(dataset=dataset)
        dsrc_coll_factory = self._service_registry.get_data_source_collection_factory(us_entry_buffer=us_entry_buffer)

        # sources
        updated_own_source_ids = []
        added_own_source_ids = []
        handled_source_ids = set()
        old_src_coll: Optional[DataSourceCollectionBase]
        for source_data in body.get("sources", []):
            source_id = source_data["id"]
            title = source_data.get("title") or None
            connection_id = source_data["connection_id"]
            source_type = source_data["source_type"]
            valid = source_data["valid"]
            handled_source_ids.add(source_id)

            # check whether it exists already
            old_dsrc_coll_spec = ds_accessor.get_data_source_coll_spec_opt(source_id=source_id)

            if old_dsrc_coll_spec is not None:
                old_src_coll = dsrc_coll_factory.get_data_source_collection(spec=old_dsrc_coll_spec)
                LOGGER.info("Source %s already exists, checking if it needs to be updated.", source_id)
                old_hash = old_src_coll.get_param_hash()
                new_hash = get_parameters_hash(
                    connection_id=connection_id,
                    source_type=source_type,
                    **source_data["parameters"],
                )  # not that this does not include title and raw_schema updates
                old_raw_schema = old_src_coll.get_cached_raw_schema(role=DataSourceRole.origin)
                schema_updated = not are_raw_schemas_same(old_raw_schema, source_data["raw_schema"])  # type: ignore  # TODO: fix

                old_index_info_set = old_src_coll.get_strict(role=DataSourceRole.origin).saved_index_info_set
                new_index_info_set = source_data["index_info_set"]
                indexes_updated = old_index_info_set != new_index_info_set

                should_update = new_hash != old_hash or schema_updated or indexes_updated
                # To do not mark source as changed if only indexes was changed
                should_update_and_notify = new_hash != old_hash or schema_updated

                assert not (should_update_and_notify and not should_update)

                if should_update:
                    # source really was updated
                    LOGGER.info("Source %s is an in-dataset source. It will be updated.", source_id)
                    ds_editor.update_data_source(
                        source_id=source_id,
                        role=DataSourceRole.origin,
                        created_from=source_type,
                        connection_id=connection_id,
                        raw_schema=source_data["raw_schema"],
                        index_info_set=new_index_info_set,
                        **source_data["parameters"],
                    )
                    if should_update_and_notify:
                        updated_own_source_ids.append(source_id)

                if old_src_coll is not None and (title != old_src_coll.title or valid != old_src_coll.valid):
                    LOGGER.info("Collection data %s has changed. Updating.", source_id)
                    ds_editor.update_data_source_collection(source_id=source_id, title=title, valid=valid)

            if old_dsrc_coll_spec is None:
                # data source doesn't exist - it has to be (re)created
                LOGGER.info("Creating new plain source %s for connection %s.", source_id, connection_id)
                ds_editor.add_data_source_collection(
                    source_id=source_id,
                    title=title,
                    managed_by=source_data["managed_by"],
                    valid=valid,
                )
                ds_editor.add_data_source(
                    source_id=source_id,
                    role=DataSourceRole.origin,
                    created_from=source_data["source_type"],
                    connection_id=connection_id,
                    raw_schema=source_data["raw_schema"],
                    index_info_set=source_data["index_info_set"],
                    parameters=source_data["parameters"],
                )
                added_own_source_ids.append(source_id)

        return updated_own_source_ids, added_own_source_ids, handled_source_ids

    @classmethod
    def _update_dataset_source_avatars_from_body(cls, dataset: Dataset, body: dict) -> tuple[str, set]:
        ds_accessor = DatasetComponentAccessor(dataset=dataset)
        ds_editor = DatasetComponentEditor(dataset=dataset)
        # source avatars
        root_avatar_id = None
        handled_source_avatar_ids = set()
        for avatar_data in body.get("source_avatars", []):
            handled_source_avatar_ids.add(avatar_data["id"])
            if ds_accessor.has_avatar(avatar_id=avatar_data["id"]):
                ds_editor.update_avatar(
                    avatar_id=avatar_data["id"],
                    source_id=avatar_data["source_id"],
                    title=avatar_data["title"],
                )
            else:
                ds_editor.add_avatar(
                    avatar_id=avatar_data["id"],
                    source_id=avatar_data["source_id"],
                    title=avatar_data["title"],
                    managed_by=avatar_data["managed_by"],
                )
            if avatar_data["is_root"]:
                root_avatar_id = avatar_data["id"]
        return root_avatar_id, handled_source_avatar_ids  # type: ignore  # TODO: fix

    @classmethod
    def _update_dataset_source_avatar_relations_from_body(cls, dataset: Dataset, body: dict) -> set:
        ds_accessor = DatasetComponentAccessor(dataset=dataset)
        ds_editor = DatasetComponentEditor(dataset=dataset)
        # avatar relations
        handled_avatar_relation_ids = set()
        for relation_data in body.get("avatar_relations", []):
            handled_avatar_relation_ids.add(relation_data["id"])
            if ds_accessor.has_avatar_relation(relation_id=relation_data["id"]):
                ds_editor.update_avatar_relation(
                    relation_id=relation_data["id"],
                    conditions=relation_data["conditions"],
                    join_type=relation_data["join_type"],
                )
            else:
                ds_editor.add_avatar_relation(
                    relation_id=relation_data["id"],
                    left_avatar_id=relation_data["left_avatar_id"],
                    right_avatar_id=relation_data["right_avatar_id"],
                    conditions=relation_data["conditions"],
                    join_type=relation_data["join_type"],
                    managed_by=relation_data["managed_by"],
                )
        return handled_avatar_relation_ids

    @staticmethod
    def _rls_list_to_set(rls_list):  # type: ignore  # TODO: fix
        return set(
            (
                rlse.field_guid,
                rlse.allowed_value,
                rlse.subject.subject_name,
                rlse.pattern_type,
            )
            for rlse in rls_list
        )

    def _update_dataset_rls_from_body(self, dataset: Dataset, body: dict, allow_rls_change: bool = True) -> None:
        if not allow_rls_for_dataset(dataset):
            return

        subject_resolver = None
        if allow_rls_change:
            # E.g. dataset editing in sync api
            subject_resolver = await_sync(self._service_registry.get_subject_resolver())

        for field in dataset.result_schema:
            rls_text_config = body["rls"].get(field.guid, "")
            # TODO: split into "pre-parse all fields", "fetch all names", "make field results",
            # to combine name-to-info fetching for all fields.
            if allow_rls_change:
                rls_entries = FieldRLSSerializer.from_text_config(
                    rls_text_config,
                    field.guid,
                    subject_resolver=subject_resolver,
                )
                dataset.rls.items = [rlse for rlse in dataset.rls.items if rlse.field_guid != field.guid]
                dataset.rls.items.extend(rls_entries)

            else:
                # e.g. preview request in async api
                rls_entries_pre = FieldRLSSerializer.from_text_config(
                    rls_text_config, field.guid, subject_resolver=None
                )
                saved_field_rls = [
                    rlse.ensure_removed_failed_subject_prefix()
                    for rlse in dataset.rls.items
                    if rlse.field_guid == field.guid
                ]
                if self._rls_list_to_set(saved_field_rls) != self._rls_list_to_set(rls_entries_pre):
                    raise exc.RLSConfigParsingError(
                        "For this feature to work, save dataset after editing the RLS config.", details=dict()
                    )
                # otherwise no effective config changes (that are worth checking in preview)

    @classmethod
    def _update_dataset_obligatory_filters_from_body(cls, dataset: Dataset, body: dict) -> str:  # type: ignore  # TODO: fix
        # obligatory_filters
        ds_accessor = DatasetComponentAccessor(dataset=dataset)
        ds_editor = DatasetComponentEditor(dataset=dataset)
        handled_oblig_filter_ids = set()
        for filter_info in body.get("obligatory_filters") or ():
            handled_oblig_filter_ids.add(filter_info.id)
            default_where_clauses = [
                DefaultWhereClause(operation=default_filter.operation, values=default_filter.values)
                for default_filter in filter_info.default_filters
            ]

            filter_by_field_guid = ds_accessor.get_obligatory_filter_opt(field_guid=filter_info.field_guid)
            if filter_by_field_guid is not None and filter_by_field_guid.id != filter_info.id:
                ds_editor.remove_obligatory_filter(obfilter_id=filter_by_field_guid.id)

            if ds_accessor.has_obligatory_filter(obfilter_id=filter_info.id):
                ds_editor.update_obligatory_filter(
                    obfilter_id=filter_info.id, default_filters=default_where_clauses, valid=filter_info.valid
                )
            else:
                ds_editor.add_obligatory_filter(
                    obfilter_id=filter_info.id,
                    field_guid=filter_info.field_guid,
                    default_filters=default_where_clauses,
                    managed_by=filter_info.managed_by,
                    valid=filter_info.valid,
                )
        for oblig_filter in ds_accessor.get_obligatory_filter_list():
            if oblig_filter.id not in handled_oblig_filter_ids:
                ds_editor.remove_obligatory_filter(obfilter_id=oblig_filter.id)

    @generic_profiler("populate-dataset-from-body")
    def populate_dataset_from_body(
        self,
        dataset: Dataset,
        us_manager: USManagerBase,
        body: dict,
        allow_rls_change: bool = True,
    ) -> DatasetUpdateInfo:
        """
        Synchronize dataset object with configuration received in ``body``.
        """

        self._ensure_additional_connections_are_preloaded(dataset_data=body, us_manager=us_manager)

        ds_accessor = DatasetComponentAccessor(dataset=dataset)
        ds_editor = DatasetComponentEditor(dataset=dataset)

        if dataset.revision_id != body["revision_id"]:
            raise exc.DatasetRevisionMismatch()

        # fields (result_schema)
        ds_editor.set_result_schema(body.get("result_schema", []))
        if body.get("result_schema_aux"):
            dataset.data.result_schema_aux = body["result_schema_aux"]

        updated_own_source_ids, added_own_source_ids, handled_source_ids = self._update_dataset_sources_from_body(
            dataset=dataset, body=body, us_entry_buffer=us_manager.get_entry_buffer()
        )
        root_avatar_id, handled_source_avatar_ids = self._update_dataset_source_avatars_from_body(
            dataset=dataset, body=body
        )
        handled_avatar_relation_ids = self._update_dataset_source_avatar_relations_from_body(dataset=dataset, body=body)

        # cleanup (done in reverse order because of dependencies)
        for relation in ds_accessor.get_avatar_relation_list():
            if relation.id not in handled_avatar_relation_ids:
                ds_editor.remove_avatar_relation(relation_id=relation.id)
        for avatar in ds_accessor.get_avatar_list():
            if avatar.id not in handled_source_avatar_ids:
                ds_editor.remove_avatar(avatar_id=avatar.id)
        for source_id in set(ds_accessor.get_data_source_id_list()) - handled_source_ids:
            ds_editor.remove_data_source_collection(source_id=source_id)

        # additional tweaks
        if root_avatar_id is not None:
            if not ds_accessor.get_avatar_strict(root_avatar_id).is_root:
                ds_editor.set_root_avatar(avatar_id=root_avatar_id, rebuild_relations=False)

        # rls
        self._update_dataset_rls_from_body(dataset=dataset, body=body, allow_rls_change=allow_rls_change)

        # errors
        if "component_errors" in body:
            dataset.error_registry.items = deepcopy(body["component_errors"].items)
            for item in dataset.error_registry.items:
                for error in item.errors:
                    if error.code[:2] == [GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX]:
                        error.code = error.code[2:]

        self._update_dataset_obligatory_filters_from_body(dataset=dataset, body=body)

        return DatasetUpdateInfo(
            added_own_source_ids=added_own_source_ids,
            updated_own_source_ids=updated_own_source_ids,
        )

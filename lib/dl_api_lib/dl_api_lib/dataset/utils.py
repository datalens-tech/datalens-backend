from __future__ import annotations

from collections import defaultdict
import logging
from typing import (
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
)

from dl_api_lib import utils as bi_utils
from dl_api_lib.enums import USPermissionKind
from dl_constants.enums import (
    ConnectionType,
    DataSourceRole,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.editor import DatasetComponentEditor
from dl_core.data_source.collection import (
    DataSourceCollection,
    DataSourceCollectionFactory,
)
import dl_core.exc as exc
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_core.us_manager.us_manager import USManagerBase


LOGGER = logging.getLogger(__name__)


def _iter_data_source_collections(
    dataset: Dataset,
    us_entry_buffer: USEntryBuffer,
    source_ids: Optional[Iterable[str]] = None,
) -> Generator[DataSourceCollection, None, None]:
    ds_accessor = DatasetComponentAccessor(dataset=dataset)

    if source_ids is None:
        source_ids = ds_accessor.get_data_source_id_list()
    assert source_ids is not None

    dsrc_coll_factory = DataSourceCollectionFactory(us_entry_buffer=us_entry_buffer)
    dataset_parameter_values = ds_accessor.get_parameter_values()
    dataset_template_enabled = ds_accessor.get_template_enabled()

    for source_id in source_ids:
        dsrc_coll_spec = ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)

        dsrc_coll = dsrc_coll_factory.get_data_source_collection(
            spec=dsrc_coll_spec,
            dataset_parameter_values=dataset_parameter_values,
            dataset_template_enabled=dataset_template_enabled,
        )
        yield dsrc_coll


def check_permissions_for_origin_sources(
    dataset: Dataset,
    source_ids: Iterable[str],
    permission_kind: USPermissionKind,
    us_entry_buffer: USEntryBuffer,
) -> None:
    """Check whether data source has read rights or not."""
    for dsrc_coll in _iter_data_source_collections(
        dataset=dataset,
        us_entry_buffer=us_entry_buffer,
        source_ids=source_ids,
    ):
        data_source = dsrc_coll.get_opt(role=DataSourceRole.origin)
        if data_source is not None:
            try:
                bi_utils.need_permission_on_entry(data_source.connection, permission_kind)
            except exc.ReferencedUSEntryNotFound:
                LOGGER.info(f"Connection for source {data_source.id} not found => skipping permission check")


def log_dataset_field_stats(dataset: Dataset) -> None:
    stats = defaultdict(int)
    stats["total"] = len(dataset.result_schema)
    for field in dataset.result_schema:
        # We expect no name collisions here
        stats[field.calc_mode.name] += 1
        stats[field.type.name.lower()] += 1
    LOGGER.info("Dataset field stats", extra=dict(dataset_field_stats=dict(stats)))


def allow_rls_for_dataset(dataset: Dataset) -> bool:
    return (
        dataset.permissions is None
        or dataset.permissions[  # dataset is not "real" (saved to US), used for validation
            "edit"
        ]  # dataset is US-bound and should have the required permissions
    )


def get_dataset_conn_types(
    dataset: Dataset,
    us_entry_buffer: USEntryBuffer,
    roles: Tuple[DataSourceRole] = (DataSourceRole.origin,),
) -> List[ConnectionType]:
    colls = list(_iter_data_source_collections(dataset=dataset, us_entry_buffer=us_entry_buffer))
    if not colls:
        return []
    result = []
    for dsrc_coll in colls:
        for role in roles:
            dsrc = dsrc_coll.get_opt(role)
            if dsrc is None:
                continue
            assert dsrc.conn_type is not None
            result.append(dsrc.conn_type)
    return result


def invalidate_sample_sources(dataset: Dataset, source_ids: List[str], us_manager: USManagerBase) -> None:
    """
    Remove sample data sources for given source IDs.
    """
    ds_editor = DatasetComponentEditor(dataset=dataset)
    for dsrc_coll in _iter_data_source_collections(
        dataset=dataset,
        us_entry_buffer=us_manager.get_entry_buffer(),
        source_ids=source_ids,
    ):
        source_id = dsrc_coll.id
        if dsrc_coll.exists(DataSourceRole.sample):
            ds_editor.remove_data_source(source_id=source_id, role=DataSourceRole.sample)

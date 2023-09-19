from __future__ import annotations

from functools import lru_cache
from itertools import chain
import logging
from typing import (
    TYPE_CHECKING,
    Collection,
    Dict,
    FrozenSet,
    Optional,
    Set,
    Tuple,
)

import attr

from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    DataSourceRole,
    JoinType,
    SourceBackendType,
)
from dl_core.backend_types import get_backend_type
from dl_core.components.accessor import DatasetComponentAccessor
import dl_core.data_source as data_source
from dl_core.data_source.type_mapping import list_registered_source_types
import dl_core.exc as exc
from dl_core.us_connection import CONNECTION_TYPES
from dl_core.us_dataset import Dataset


if TYPE_CHECKING:
    from dl_core.data_source.collection import (
        DataSourceCollectionBase,
        DataSourceCollectionFactory,
    )


LOGGER = logging.getLogger(__name__)


@lru_cache(maxsize=200)
def get_compatible_source_types(source_type: CreateDSFrom) -> FrozenSet[CreateDSFrom]:
    """Return frozen set of data source types compatible with ``ds_type``"""

    raw_comp_types = frozenset(list_registered_source_types())

    dsrc_cls = data_source.get_data_source_class(ds_type=source_type)
    compat_types = {
        comp_source_type
        for comp_source_type in raw_comp_types
        if dsrc_cls.is_compatible_with_type(source_type=comp_source_type)
    }
    return frozenset(compat_types)


_SOURCE_CONNECTION_COMPATIBILITY: Dict[CreateDSFrom, FrozenSet[ConnectionType]] = {}


def _populate_compatibility_map():  # type: ignore  # TODO: fix
    for conn_type, conn_cls in CONNECTION_TYPES.items():
        for dsrc_type in conn_cls.get_provided_source_types():
            _SOURCE_CONNECTION_COMPATIBILITY[dsrc_type] = _SOURCE_CONNECTION_COMPATIBILITY.get(
                dsrc_type, frozenset()
            ) | frozenset([conn_type])


@lru_cache(maxsize=100)
def get_conn_types_compatible_with_src_types(source_types: FrozenSet[CreateDSFrom]) -> FrozenSet[ConnectionType]:
    if not _SOURCE_CONNECTION_COMPATIBILITY:
        _populate_compatibility_map()
    assert _SOURCE_CONNECTION_COMPATIBILITY
    return frozenset(
        chain.from_iterable(_SOURCE_CONNECTION_COMPATIBILITY.get(dsrc_type, frozenset()) for dsrc_type in source_types)
    )


@attr.s
class DatasetCapabilities:
    _dataset: Dataset = attr.ib(kw_only=True)
    _dsrc_coll_factory: DataSourceCollectionFactory = attr.ib(kw_only=True)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    def _get_data_source_strict(self, source_id: str, role: DataSourceRole) -> data_source.DataSource:
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
        dsrc_coll = self._dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)
        dsrc = dsrc_coll.get_strict(role=role)
        return dsrc

    def _get_first_dsrc_collection(
        self,
        ignore_source_ids: Optional[Collection[str]] = None,
    ) -> Optional["data_source.DataSourceCollectionBase"]:
        source_id = self._dataset.get_single_data_source_id(ignore_source_ids=ignore_source_ids)
        if source_id is not None:
            dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_opt(source_id)
            if dsrc_coll_spec is not None:
                return self._dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)

        return None

    def get_effective_connection_id(  # type: ignore  # TODO: fix
        self,
        ignore_source_ids: Optional[Collection[str]] = None,
    ) -> Optional[str]:
        dsrc_coll = self._get_first_dsrc_collection(ignore_source_ids=ignore_source_ids)
        if dsrc_coll is not None:
            return dsrc_coll.effective_connection_id

    def get_supported_join_types(
        self,
        ignore_source_ids: Optional[Collection[str]] = None,
    ) -> Set[JoinType]:
        ignore_source_ids = ignore_source_ids or ()
        role = self.resolve_source_role(ignore_source_ids=ignore_source_ids)
        result = set(jt for jt in JoinType)
        for source_id in self._ds_accessor.get_data_source_id_list():
            if source_id in ignore_source_ids:
                continue
            result &= self._get_data_source_strict(source_id=source_id, role=role).supported_join_types
        return result

    def source_can_be_added(
        self,
        connection_id: Optional[str],
        created_from: CreateDSFrom,
        ignore_source_ids: Optional[Collection[str]] = None,
    ) -> bool:
        """
        Check whether a data source with given connection and type can be added to the dataset.

        ``ignore_source_ids`` can be specified to exclude certain exising sources from all checks.
        This can be useful for checking whether a source can be replaced with a new one
        """

        ignore_source_ids = ignore_source_ids or ()
        dsrc_cls = data_source.get_data_source_class(ds_type=created_from)
        existing_source_id = self._dataset.get_single_data_source_id(ignore_source_ids=ignore_source_ids)
        effective_connection_id = self.get_effective_connection_id(ignore_source_ids=ignore_source_ids)
        supported_join_types = self.get_supported_join_types(ignore_source_ids=ignore_source_ids)

        if existing_source_id is None:  # dataset is empty
            return True

        if not supported_join_types & dsrc_cls.supported_join_types:
            return False

        return connection_id == effective_connection_id

    def get_compatible_source_types(
        self,
        ignore_source_ids: Optional[Collection[str]] = None,
    ) -> FrozenSet[CreateDSFrom]:
        """Return a frozen set of source types compatible with dataset's current state"""

        ignore_source_ids = ignore_source_ids or ()
        # All available source types
        comp_types = frozenset(list_registered_source_types())
        # Now check them for compatibility with existing sources
        for source_id in self._ds_accessor.get_data_source_id_list():
            if source_id in ignore_source_ids:
                continue
            dsrc = self._get_data_source_strict(source_id=source_id, role=DataSourceRole.origin)
            dsrc_type = dsrc.spec.source_type
            comp_types &= get_compatible_source_types(dsrc_type)
        return comp_types

    def _get_data_source_collections(self) -> dict[str, DataSourceCollectionBase]:
        result = {}
        for dsrc_coll_spec in self._ds_accessor.get_data_source_coll_spec_list():
            result[dsrc_coll_spec.id] = self._dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)
        return result

    def get_compatible_connection_types(
        self,
        ignore_connection_ids: Optional[Collection[str]] = None,
    ) -> FrozenSet[ConnectionType]:
        """Return a frozen set of connection types compatible with dataset's current state"""

        ignore_connection_ids = ignore_connection_ids or ()
        ignore_source_ids = {
            # all sources that use the ignored connections
            dsrc_coll.id
            for dsrc_coll in self._get_data_source_collections().values()
            if dsrc_coll.get_connection_id(role=DataSourceRole.origin) in ignore_connection_ids
        }

        # No two connections can be used together in a single non-materialized dataset
        existing_source_ids = {
            source_id for source_id in self._ds_accessor.get_data_source_id_list() if source_id not in ignore_source_ids
        }
        if existing_source_ids:
            # A connection is already present in the dataset. Can't add a second one, no matter what type.
            return frozenset()

        # Dataset is empty (taking into account ignore_source_ids)
        return get_conn_types_compatible_with_src_types(frozenset(list_registered_source_types()))

    def supports_offset(self, role: DataSourceRole) -> bool:
        for _source_id, dsrc_coll in self._get_data_source_collections().items():
            dsrc = dsrc_coll.get_strict(role=role)
            if not dsrc.supports_offset:
                return False
        return True

    def get_backend_type(self, role: DataSourceRole) -> SourceBackendType:
        source_id = self._dataset.get_single_data_source_id()
        assert source_id is not None
        dsrc = self._get_data_source_strict(source_id=source_id, role=role)
        assert dsrc is not None
        return get_backend_type(conn_type=dsrc.conn_type)

    def resolve_source_role(
        self,
        for_preview: bool = False,
        ignore_source_ids: Optional[Collection[str]] = None,
        log_reasons: bool = False,
    ) -> DataSourceRole:
        """
        Resolve role that should be used for all source collections
        """
        collections = list(self._get_data_source_collections().values())
        if not collections:
            return DataSourceRole.origin

        # dict: {role: sum_of_priority_values}
        common_priorities = {role: 0 for role in DataSourceRole}
        for coll in collections:
            if ignore_source_ids is not None and coll.id in ignore_source_ids:
                continue
            role_resolution_info = coll.resolve_role_priorities(for_preview=for_preview)
            coll_priorities = role_resolution_info.priorities

            if log_reasons:
                LOGGER.info(
                    f"Role resolution_info for data source {coll.id}",
                    extra=dict(
                        role_resolution_info=dict(
                            source_id=coll.id,
                            source_type=coll.source_type.name,
                            origin=role_resolution_info.origin.name,
                            sample=role_resolution_info.sample.name,
                            materialization=role_resolution_info.materialization.name,
                        )
                    ),
                )

            for role in common_priorities.copy():
                if role not in coll_priorities:
                    del common_priorities[role]
                else:
                    common_priorities[role] += coll_priorities.index(role)

        if not common_priorities:
            raise exc.NoCommonRoleError()

        def role_sorting_key(role_priority_pair: Tuple[DataSourceRole, int]) -> Tuple[int, str]:
            # first priority, then name (so that it's deterministic for matching priorities)
            return role_priority_pair[1], role_priority_pair[0].name

        sorted_role_priorities = sorted(common_priorities.items(), key=role_sorting_key)
        # at this point we know it's not empty
        role, _ = sorted_role_priorities[0]
        return role

    def supports_preview(self) -> bool:
        """
        Check whether dataset supports preview.
        Used in options to tell the UI (or some other client) whether preview should/could be shown.
        """
        for _source_id, dsrc_coll in self._get_data_source_collections().items():
            if not dsrc_coll.get_strict().preview_enabled:
                return False

        return True

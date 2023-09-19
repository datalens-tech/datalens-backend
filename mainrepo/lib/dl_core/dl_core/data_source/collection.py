from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Optional,
    Union,
)

import attr

from dl_constants.enums import (
    CreateDSFrom,
    DataSourceCollectionType,
    DataSourceRole,
    JoinType,
    ManagedBy,
)
from dl_core.base_models import (
    ConnectionRef,
    DefaultConnectionRef,
    InternalMaterializationConnectionRef,
)
from dl_core.data_source import (
    base,
    type_mapping,
)
from dl_core.data_source.utils import get_parameters_hash
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.data_source_spec.collection import (
    DataSourceCollectionSpec,
    DataSourceCollectionSpecBase,
)
from dl_core.db import SchemaColumn
from dl_core.enums import RoleReason
import dl_core.exc as exc


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase
    from dl_core.us_connection_base import ConnectionBase
    from dl_core.us_manager.local_cache import USEntryBuffer


@attr.s(slots=True)
class RoleResolutionInfo:
    origin: RoleReason = attr.ib(kw_only=True, default=RoleReason.not_needed)
    materialization: RoleReason = attr.ib(kw_only=True, default=RoleReason.not_needed)
    sample: RoleReason = attr.ib(kw_only=True, default=RoleReason.not_needed)
    priorities: list[DataSourceRole] = attr.ib(kw_only=True, factory=list)


LazyDataSourceType = Union[base.DataSource, dict]  # data source instance or param dict


@attr.s
class DataSourceCollectionBase:
    dsrc_coll_type: ClassVar[DataSourceCollectionType]
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)

    _spec: DataSourceCollectionSpecBase = attr.ib(default=None)

    @property
    def spec(self) -> DataSourceCollectionSpecBase:
        assert isinstance(self._spec, DataSourceCollectionSpecBase)
        return self._spec

    @property
    def id(self) -> str:
        return self.spec.id

    @property
    def valid(self) -> bool:
        return self.spec.valid

    @property
    def title(self) -> Optional[str]:
        return self.spec.title

    @property
    def managed_by(self) -> ManagedBy:
        return self.spec.managed_by

    def get_connection_id(self, role: Optional[DataSourceRole] = None) -> Optional[str]:
        conn_ref = self.get_strict(role=role).connection_ref
        if isinstance(conn_ref, DefaultConnectionRef):
            return conn_ref.conn_id
        elif isinstance(conn_ref, InternalMaterializationConnectionRef):
            return None
        else:
            raise TypeError(f"Unexpected conn_ref class: {type(conn_ref)}")

    @property
    def effective_connection_id(self) -> Optional[str]:
        return self.get_connection_id()

    @property
    def source_type(self) -> CreateDSFrom:
        return self.get_strict().spec.source_type

    def supports_join_type(
        self,
        join_type: JoinType,
        role: Optional[DataSourceRole] = None,
        for_preview: bool = False,
    ) -> bool:
        if role is None:
            role = self.resolve_role(for_preview=for_preview)
        dsrc = self.get_strict(role=role)
        return join_type in dsrc.supported_join_types

    def _get_connection_by_ref(self, connection_ref: ConnectionRef) -> ConnectionBase:
        from dl_core.us_connection_base import ConnectionBase  # to avoid circular imports

        connection = self._us_entry_buffer.get_entry(connection_ref)
        assert isinstance(connection, ConnectionBase)
        return connection

    def __contains__(self, role: DataSourceRole) -> bool:
        raise NotImplementedError

    def exists(self, role: DataSourceRole) -> bool:
        raise NotImplementedError

    def invalidate(self, role: DataSourceRole) -> None:
        pass

    def are_schemas_compatible(self, role_1: DataSourceRole, role_2: DataSourceRole) -> bool:
        """
        Compare raw schemas of two sources and decide whether they are compatible for data selection
        """

        assert role_1 is DataSourceRole.origin, "First role in comparison must be origin"
        assert role_2 in (
            DataSourceRole.materialization,
            DataSourceRole.sample,
        ), "Second role must be some kind of materialization (materialization or sample)"

        # Origin never uses the materialization connection,
        # so we can safely load it
        src_1 = self.get_opt(role=role_1)
        # materialization/sample sources, however, MUST use it
        # and they must be SQL sources, which means we can get the schema from the spec
        src_2_spec = self._get_spec_for_role(role_2)
        if src_1 is None or src_2_spec is None:
            return False

        raw_schema_1 = sorted(src_1.saved_raw_schema or (), key=lambda col: col.name)
        raw_schema_2 = sorted(src_2_spec.raw_schema or (), key=lambda col: col.name)
        if len(raw_schema_1) != len(raw_schema_2):
            return False

        for col_1, col_2 in zip(raw_schema_1, raw_schema_2):
            if col_1.name != col_2.name or col_1.user_type != col_2.user_type:
                return False

        return True

    def resolve_role_priorities(self, for_preview: bool = False) -> RoleResolutionInfo:
        """
        Decide, which source roles can be used for main source or preview
        taking into account the current configuration and states
        of the collection and individual data sources.
        Return them as a list with items at the beginning having top priority
        """

        resolution_info = RoleResolutionInfo()
        feature_managed = self.managed_by == ManagedBy.feature

        if for_preview:
            sample_spec = self._get_spec_for_role(role=DataSourceRole.sample)
            if sample_spec is None:
                resolution_info.sample = RoleReason.missing_source
            elif not self.get_strict(DataSourceRole.origin).supports_sample_usage:
                resolution_info.sample = RoleReason.not_supported
            elif not sample_spec.is_configured:
                resolution_info.sample = RoleReason.not_configured
            # At this point sample is materialized and ready for use.
            elif not feature_managed and not self.are_schemas_compatible(DataSourceRole.origin, DataSourceRole.sample):
                resolution_info.sample = RoleReason.schema_mismatch
            else:
                # structure is the same, so we can use the existing sample data source
                resolution_info.sample = RoleReason.selected
                resolution_info.priorities.append(DataSourceRole.sample)

            # either there is no sample that is ready for usage or source's structure has changed in origin

        # all other cases (including main source for direct mode)
        if feature_managed:
            resolution_info.origin = RoleReason.forbidden_for_features
        else:
            resolution_info.origin = RoleReason.selected
            resolution_info.priorities.append(DataSourceRole.origin)

        return resolution_info

    def resolve_role(self, for_preview: bool = False) -> DataSourceRole:
        """
        Decide, which source role to use for main source or preview
        taking into account the current configuration and states
        of the collection and individual data sources.
        """
        role_priorities = self.resolve_role_priorities(for_preview=for_preview).priorities
        if not role_priorities:
            raise exc.NoCommonRoleError()
        return role_priorities[0]

    def get_opt(self, role: Optional[DataSourceRole] = None, for_preview: bool = False) -> Optional["base.DataSource"]:
        raise NotImplementedError

    def get_strict(self, role: Optional[DataSourceRole] = None, for_preview: bool = False) -> base.DataSource:
        dsrc = self.get_opt(role=role, for_preview=for_preview)
        assert dsrc is not None
        return dsrc

    def _get_spec_for_role(self, role: DataSourceRole) -> Optional[DataSourceSpec]:
        raise NotImplementedError

    def get_cached_raw_schema(
        self,
        role: Optional[DataSourceRole] = None,
        for_preview: bool = False,
    ) -> Optional[list[SchemaColumn]]:
        dsrc = self.get_strict(role=role, for_preview=for_preview)
        raw_schema: Optional[list[SchemaColumn]] = dsrc.saved_raw_schema or []
        if not raw_schema and role != DataSourceRole.origin:
            # we need to at least have BI types, so deduce them from the original raw schema
            origin_dsrc = self.get_strict(role=DataSourceRole.origin)
            origin_columns = origin_dsrc.saved_raw_schema
            if origin_columns is None:
                return None

            # "import" raw schema from origin dsrc to current one
            tt = origin_dsrc.type_transformer
            raw_schema = [
                col.clone(  # type: ignore  # TODO: fix
                    # convert data type to match data source type
                    user_type=col.user_type
                    or tt.type_native_to_user(native_t=col.native_type),
                )
                for col in origin_columns
            ]

        raw_schema = self._patch_raw_schema(raw_schema)
        return raw_schema

    def get_raw_schema(
        self,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
        role: Optional[DataSourceRole] = None,
        for_preview: bool = False,
    ) -> Optional[list[SchemaColumn]]:
        """
        Return raw schema of data source.
        Since the underlying data source might be a reference to an external one,
        its ID may be different to ``self._id``, so all columns need to be patched
        with the correct ID (see end of method).

        :param from_cache: return raw_schema saved in config
        :param role: which data source in collection
        :param for_preview: preview mode
        :return:
        """

        dsrc = self.get_strict(role=role, for_preview=for_preview)
        raw_schema: Optional[list[SchemaColumn]] = dsrc.get_schema_info(
            conn_executor_factory=conn_executor_factory
        ).schema
        raw_schema = self._patch_raw_schema(raw_schema)
        return raw_schema

    def _patch_raw_schema(self, raw_schema: Optional[list[SchemaColumn]]) -> Optional[list[SchemaColumn]]:
        """Patch all raw_schema columns with own source_id"""
        if raw_schema is None:
            return None

        return [col.clone(source_id=self.id) for col in raw_schema]

    def get_param_hash(self) -> str:
        raise NotImplementedError


@attr.s
class DataSourceCollection(DataSourceCollectionBase):
    dsrc_coll_type = DataSourceCollectionType.collection

    _loaded_sources: dict[DataSourceRole, Optional[base.DataSource]] = attr.ib(init=False, factory=dict)

    @property
    def spec(self) -> DataSourceCollectionSpec:
        assert isinstance(self._spec, DataSourceCollectionSpec)
        return self._spec

    def _initialize_dsrc(self, dsrc_spec: DataSourceSpec) -> "base.DataSource":
        dsrc_cls = type_mapping.get_data_source_class(dsrc_spec.source_type)
        dsrc = dsrc_cls(id=self.id, us_entry_buffer=self._us_entry_buffer, spec=dsrc_spec)
        return dsrc

    def _get_spec_for_role(self, role: DataSourceRole) -> Optional[DataSourceSpec]:
        if role == DataSourceRole.origin:
            return self.spec.origin
        if role == DataSourceRole.materialization:
            return self.spec.materialization
        if role == DataSourceRole.sample:
            return self.spec.sample
        raise ValueError(f"Invalid role: {role}")

    def __contains__(self, role: DataSourceRole) -> bool:
        return self._get_spec_for_role(role) is not None

    def get_opt(self, role: Optional[DataSourceRole] = None, for_preview: bool = False) -> Optional["base.DataSource"]:
        """Return data source for role. Initialize it if it isn't initialized yet"""
        if role is None:
            role = self.resolve_role(for_preview=for_preview)
        assert role is not None
        if role not in self:
            return None
        if role not in self._loaded_sources:
            dsrc_spec = self._get_spec_for_role(role=role)
            if dsrc_spec is None:
                self._loaded_sources[role] = None
            else:
                self._loaded_sources[role] = self._initialize_dsrc(dsrc_spec=dsrc_spec)
        return self._loaded_sources[role]

    def exists(self, role: DataSourceRole) -> bool:
        return self.get_opt(role) is not None

    def invalidate(self, role: DataSourceRole) -> None:
        try:
            del self._loaded_sources[role]
        except KeyError:
            pass

    def get_param_hash(self) -> str:
        role = DataSourceRole.origin
        dsrc = self.get_strict(role)
        return get_parameters_hash(
            source_type=dsrc.spec.source_type,
            connection_id=self.get_connection_id(role=role),
            **dsrc.get_parameters(),
        )


@attr.s
class DataSourceCollectionFactory:  # TODO: Move to service_registry
    _us_entry_buffer: USEntryBuffer = attr.ib(default=None)

    def get_data_source_collection(self, spec: DataSourceCollectionSpecBase) -> DataSourceCollectionBase:
        if isinstance(spec, DataSourceCollectionSpec):
            return DataSourceCollection(us_entry_buffer=self._us_entry_buffer, spec=spec)
        raise TypeError(f"Invalid spec type: {spec})")

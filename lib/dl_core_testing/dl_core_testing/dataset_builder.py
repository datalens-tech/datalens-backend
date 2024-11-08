from __future__ import annotations

import abc
import uuid

import attr

from dl_constants.enums import (
    BinaryJoinOperator,
    DataSourceRole,
    DataSourceType,
)
from dl_core.base_models import DefaultConnectionRef
from dl_core.data_source.base import DataSource
from dl_core.multisource import (
    AvatarRelation,
    BinaryCondition,
    ConditionPartDirect,
    SourceAvatar,
)
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import (
    Db,
    DbTable,
    make_table,
)
from dl_core_testing.dataset_wrappers import EditableDatasetTestWrapper


@attr.s(frozen=True)
class DataSourceCreationSpec:
    connection: ConnectionBase = attr.ib(kw_only=True)
    source_type: DataSourceType = attr.ib(kw_only=True)
    dsrc_params: dict = attr.ib(kw_only=True)


@attr.s
class DatasetSourceGenerator(abc.ABC):
    @abc.abstractmethod
    def generate_source_params(self) -> DataSourceCreationSpec:
        raise NotImplementedError


@attr.s
class DefaultDbDatasetSourceGenerator(DatasetSourceGenerator):
    db: Db = attr.ib(kw_only=True)
    connection: ConnectionBase = attr.ib(kw_only=True)

    def _get_table_source_params(self, db_table: DbTable) -> dict:
        return dict(
            db_name=db_table.db.name,
            schema_name=db_table.schema,
            table_name=db_table.name,
        )

    def generate_source_params(self) -> DataSourceCreationSpec:
        db_table = make_table(self.db)
        source_type = self.connection.source_type
        return DataSourceCreationSpec(
            connection=self.connection, source_type=source_type, dsrc_params=self._get_table_source_params(db_table)
        )


@attr.s(frozen=True)
class DatasetBuilderComponentProxy:
    dataset_builder: DatasetBuilder = attr.ib(kw_only=True)


@attr.s(frozen=True)
class DataSourceProxy(DatasetBuilderComponentProxy):
    source_id: str = attr.ib(kw_only=True)

    @property
    def target(self) -> DataSource:
        return self.dataset_builder.ds_wrapper.get_data_source_strict(
            source_id=self.source_id, role=DataSourceRole.origin
        )

    def add_avatar(self) -> SourceAvatarProxy:
        return self.dataset_builder.add_source_avatar(source_id=self.source_id)


@attr.s(frozen=True)
class SourceAvatarProxy(DatasetBuilderComponentProxy):
    avatar_id: str = attr.ib(kw_only=True)

    @property
    def target(self) -> SourceAvatar:
        return self.dataset_builder.ds_wrapper.get_avatar_strict(avatar_id=self.avatar_id)

    def add_relation(
        self,
        *,
        right: SourceAvatarProxy | str,
        conditions: list[BinaryCondition],
    ) -> AvatarRelationProxy:
        if isinstance(right, SourceAvatarProxy):
            right = right.avatar_id
        assert isinstance(right, str)
        return self.dataset_builder.add_avatar_relation(
            left_avatar_id=self.avatar_id,
            right_avatar_id=right,
            conditions=conditions,
        )

    def add_relation_simple_eq(
        self,
        *,
        right: SourceAvatarProxy | str,
        left_col_name: str,
        right_col_name: str,
        required: bool = False,
    ) -> AvatarRelationProxy:
        if isinstance(right, SourceAvatarProxy):
            right = right.avatar_id
        assert isinstance(right, str)
        return self.dataset_builder.add_avatar_relation_simple_eq(
            left_avatar_id=self.avatar_id,
            right_avatar_id=right,
            left_col_name=left_col_name,
            right_col_name=right_col_name,
            required=required,
        )


@attr.s(frozen=True)
class AvatarRelationProxy(DatasetBuilderComponentProxy):
    relation_id: str = attr.ib(kw_only=True)

    @property
    def target(self) -> AvatarRelation:
        return self.dataset_builder.ds_wrapper.get_avatar_relation_strict(relation_id=self.relation_id)


@attr.s
class DatasetBuilder:
    service_registry: ServicesRegistry = attr.ib(kw_only=True)
    sync_us_manager: SyncUSManager = attr.ib(kw_only=True)
    dataset: Dataset = attr.ib(kw_only=True)
    dsrc_generator: DatasetSourceGenerator = attr.ib(kw_only=True, default=None)

    ds_wrapper: EditableDatasetTestWrapper = attr.ib(init=False)

    @ds_wrapper.default
    def _make_ds_wrapper(self) -> EditableDatasetTestWrapper:
        return EditableDatasetTestWrapper(
            dataset=self.dataset,
            us_entry_buffer=self.sync_us_manager.get_entry_buffer(),
        )

    def add_data_source(self, *, fill_raw_schema: bool = True) -> DataSourceProxy:
        source_id: str = str(uuid.uuid4())
        role = DataSourceRole.origin

        dsrc_creation_spec = self.dsrc_generator.generate_source_params()
        connection_id = dsrc_creation_spec.connection.uuid
        assert connection_id is not None
        self.sync_us_manager.ensure_entry_preloaded(DefaultConnectionRef(conn_id=connection_id))
        self.ds_wrapper.add_data_source(
            source_id=source_id,
            role=role,
            created_from=dsrc_creation_spec.source_type,
            connection_id=connection_id,
            parameters=dsrc_creation_spec.dsrc_params,
        )

        if fill_raw_schema:
            dsrc = self.ds_wrapper.get_data_source_strict(source_id=source_id, role=role)
            ce_factory = self.service_registry.get_conn_executor_factory()
            conn_executor = ce_factory.get_sync_conn_executor(conn=dsrc_creation_spec.connection)
            raw_schema = dsrc.get_schema_info(conn_executor_factory=lambda: conn_executor).schema
            assert raw_schema is not None
            self.ds_wrapper.update_data_source(source_id=source_id, role=role, raw_schema=raw_schema)

        return DataSourceProxy(
            dataset_builder=self,
            source_id=source_id,
        )

    def add_source_avatar(self, source_id: str) -> SourceAvatarProxy:
        avatar_id = str(uuid.uuid4())
        avatar_title = str(uuid.uuid4())
        self.ds_wrapper.add_avatar(avatar_id=avatar_id, source_id=source_id, title=avatar_title)
        return SourceAvatarProxy(
            dataset_builder=self,
            avatar_id=avatar_id,
        )

    def add_avatar_relation(
        self,
        left_avatar_id: str,
        right_avatar_id: str,
        conditions: list[BinaryCondition],
        required: bool = False,
    ) -> AvatarRelationProxy:
        relation_id = str(uuid.uuid4())
        self.ds_wrapper.add_avatar_relation(
            relation_id=relation_id,
            left_avatar_id=left_avatar_id,
            right_avatar_id=right_avatar_id,
            conditions=conditions,
            required=required,
        )
        return AvatarRelationProxy(
            dataset_builder=self,
            relation_id=relation_id,
        )

    def add_avatar_relation_simple_eq(
        self,
        left_avatar_id: str,
        right_avatar_id: str,
        left_col_name: str,
        right_col_name: str,
        required: bool = False,
    ) -> AvatarRelationProxy:
        conditions = [
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source=left_col_name),
                right_part=ConditionPartDirect(source=right_col_name),
            ),
        ]
        return self.add_avatar_relation(
            left_avatar_id=left_avatar_id,
            right_avatar_id=right_avatar_id,
            conditions=conditions,
            required=required,
        )


@attr.s
class DatasetBuilderFactory(abc.ABC):
    @abc.abstractmethod
    def get_dataset_builder(self, dataset: Dataset) -> DatasetBuilder:
        raise NotImplementedError


@attr.s
class DefaultDbDatasetBuilderFactory(DatasetBuilderFactory):
    db: Db = attr.ib(kw_only=True)
    service_registry: ServicesRegistry = attr.ib(kw_only=True)
    sync_us_manager: SyncUSManager = attr.ib(kw_only=True)
    connection: ConnectionBase = attr.ib(kw_only=True)

    def _get_dsrc_generator(self) -> DatasetSourceGenerator:
        return DefaultDbDatasetSourceGenerator(db=self.db, connection=self.connection)

    def get_dataset_builder(self, dataset: Dataset) -> DatasetBuilder:
        return DatasetBuilder(
            dataset=dataset,
            dsrc_generator=self._get_dsrc_generator(),
            service_registry=self.service_registry,
            sync_us_manager=self.sync_us_manager,
        )

from __future__ import annotations

from contextlib import contextmanager
from typing import (
    Generator,
    Optional,
)

import attr
import sqlalchemy as sa

from dl_constants.enums import DataSourceRole
from dl_core.backend_types import get_backend_type
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.ids import AvatarId
from dl_core.constants import DataAPILimits
from dl_core.data_processing.prepared_components.manager_base import PreparedComponentManagerBase
from dl_core.data_processing.prepared_components.primitives import PreparedSingleFromInfo
from dl_core.data_processing.query_compiler_registry import get_sa_query_compiler_cls
from dl_core.data_source.collection import DataSourceCollectionFactory
import dl_core.data_source.sql
import dl_core.exc as exc
from dl_core.multisource import SourceAvatar
from dl_core.query.bi_query import SqlSourceType
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_query_processing.compilation.specs import ParameterValueSpec


@attr.s
class DefaultPreparedComponentManager(PreparedComponentManagerBase):
    """
    Manages basic prepared components - ones formed from static dataset avatars and relations.
    """

    _dataset: Dataset = attr.ib(kw_only=True)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)
    _role: DataSourceRole = attr.ib(kw_only=True, default=DataSourceRole.origin)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)
    _dsrc_coll_factory: DataSourceCollectionFactory = attr.ib(init=False)
    _parameter_value_specs: list[ParameterValueSpec] | None = attr.ib(kw_only=True, default=None)

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    @_dsrc_coll_factory.default
    def _make_dsrc_coll_factory(self) -> DataSourceCollectionFactory:
        return DataSourceCollectionFactory(us_entry_buffer=self._us_entry_buffer)

    @contextmanager
    def _handle_incomplete_source(self, avatar: SourceAvatar) -> Generator[None, None, None]:
        try:
            yield
        except exc.TableNameNotConfiguredError as err:
            if self._role == DataSourceRole.materialization:
                raise exc.MaterializationNotFinished(
                    message="Materialization is not yet finished",
                    query="<get_from_clause>",
                    db_message="",
                    details={"avatar_id": avatar.id, "avatar_title": avatar.title},
                ) from err
            else:
                raise

    def get_prepared_source(
        self,
        avatar_id: AvatarId,
        alias: str,
        from_subquery: bool,
        subquery_limit: Optional[int],
    ) -> PreparedSingleFromInfo:
        avatar = self._ds_accessor.get_avatar_strict(avatar_id=avatar_id)
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=avatar.source_id)
        dataset_parameter_values = self._ds_accessor.get_parameter_values()
        if self._parameter_value_specs:
            dataset_parameter_values.update(
                self._ds_accessor.get_parameter_values_from_specs(parameter_value_specs=self._parameter_value_specs)
            )
        dsrc_coll = self._dsrc_coll_factory.get_data_source_collection(
            spec=dsrc_coll_spec,
            dataset_parameter_values=dataset_parameter_values,
            dataset_template_enabled=self._ds_accessor.get_template_enabled(),
        )
        dsrc = dsrc_coll.get_strict(role=self._role)
        if not isinstance(dsrc, dl_core.data_source.sql.BaseSQLDataSource):
            raise TypeError(f"Root data source has non-SQL type: {type(dsrc)}")

        def get_columns():  # type: ignore  # TODO: fix
            with self._handle_incomplete_source(avatar=avatar):
                columns = dsrc.saved_raw_schema
                if columns is None:
                    raise exc.TableNameNotConfiguredError()
                return columns

        columns = get_columns()
        col_names = [col.name for col in columns]
        from_subquery = from_subquery and dsrc.supports_preview_from_subquery
        connection = dsrc.connection
        conn_type = connection.conn_type
        backend_type = get_backend_type(conn_type=conn_type)
        sa_dialect = connection.get_dialect()
        query_compiler_cls = get_sa_query_compiler_cls(backend_type=backend_type)
        query_compiler = query_compiler_cls(dialect=sa_dialect)

        sql_source: SqlSourceType
        if from_subquery:
            # Subquery mode: wrap the leftmost (root) source/table into a "SELECT ... FROM ..." subquery
            # to limit the number of entries before the GROUP BY clause is executed
            fields = [sa.literal_column(query_compiler.quote(col_name)) for col_name in col_names] or ["*"]
            sql_source = (
                sa.select(fields)
                .select_from(dsrc.get_sql_source())
                .limit(subquery_limit or DataAPILimits.DEFAULT_SUBQUERY_LIMIT)
                .alias(alias)
            )
        else:
            sql_source = dsrc.get_sql_source(alias=alias)

        target_connection_ref = dsrc.connection_ref
        target_connection = self._us_entry_buffer.get_entry(target_connection_ref)
        assert isinstance(target_connection, ConnectionBase)

        data_key = dsrc.get_cache_key_part()
        data_key = data_key.extend(part_type="avatar_id", part_content=avatar_id)

        prep_src_info = PreparedSingleFromInfo(
            id=avatar_id,
            alias=alias,
            data_source_list=(dsrc,),
            col_names=col_names,
            user_types=[col.user_type for col in columns],
            sql_source=sql_source,
            query_compiler=query_compiler,
            supported_join_types=dsrc.supported_join_types,
            db_name=(dsrc.db_name if isinstance(dsrc, dl_core.data_source.sql.DbSQLDataSourceMixin) else None),
            connect_args=dsrc.get_connect_args(),
            pass_db_query_to_user=target_connection.pass_db_query_to_user,
            target_connection_ref=target_connection_ref,
            data_key=data_key,
        )
        return prep_src_info

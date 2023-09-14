from __future__ import annotations

from contextlib import contextmanager
from typing import (
    Generator,
    Optional,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql.selectable import FromClause

from bi_constants.enums import DataSourceRole
from bi_core.components.accessor import DatasetComponentAccessor
from bi_core.components.ids import AvatarId
from bi_core.constants import DataAPILimits
from bi_core.data_processing.prepared_components.manager_base import PreparedComponentManagerBase
from bi_core.data_processing.prepared_components.primitives import PreparedSingleFromInfo
from bi_core.data_source.collection import DataSourceCollectionFactory
import bi_core.data_source.sql
import bi_core.exc as exc
from bi_core.multisource import SourceAvatar
from bi_core.us_connection_base import ConnectionBase
from bi_core.us_dataset import Dataset
from bi_core.us_manager.local_cache import USEntryBuffer


@attr.s
class DefaultPreparedComponentManager(PreparedComponentManagerBase):
    """
    Manages basic prepared components - ones formed from static dataset avatars and relations.
    """

    _dataset: Dataset = attr.ib(kw_only=True)
    _role: DataSourceRole = attr.ib(kw_only=True)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)
    _dsrc_coll_factory: DataSourceCollectionFactory = attr.ib(init=False)

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
        self, avatar_id: AvatarId, alias: str, from_subquery: bool, subquery_limit: Optional[int]
    ) -> PreparedSingleFromInfo:
        avatar = self._ds_accessor.get_avatar_strict(avatar_id=avatar_id)
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=avatar.source_id)
        dsrc_coll = self._dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)
        dsrc = dsrc_coll.get_strict(role=self._role)
        if not isinstance(dsrc, bi_core.data_source.sql.BaseSQLDataSource):
            raise TypeError(f"Root data source has non-SQL type: {type(dsrc)}")

        def get_columns():  # type: ignore  # TODO: fix
            with self._handle_incomplete_source(avatar=avatar):  # type: ignore  # TODO: fix
                columns = dsrc.saved_raw_schema  # type: ignore  # TODO: fix
                if columns is None:
                    raise exc.TableNameNotConfiguredError()
                return columns

        columns = get_columns()
        col_names = [col.name for col in columns]
        from_subquery = from_subquery and dsrc.supports_preview_from_subquery
        query_compiler = dsrc.get_query_compiler()

        sql_source: FromClause
        if from_subquery:
            # Subquery mode: wrap the leftmost (root) source/table into a "SELECT ... FROM ..." subquery
            # to limit the number of entries before the GROUP BY clause is executed
            fields = [sa.literal_column(query_compiler.quote(col_name)) for col_name in col_names] or ["*"]
            sql_source = (
                sa.select(fields)
                .select_from(dsrc.get_sql_source())  # type: ignore  # TODO: fix
                .limit(subquery_limit or DataAPILimits.DEFAULT_SUBQUERY_LIMIT)
                .alias(alias)
            )
        else:
            sql_source = dsrc.get_sql_source(alias=alias)  # type: ignore  # TODO: fix

        target_connection_ref = dsrc.connection_ref
        target_connection = self._us_entry_buffer.get_entry(target_connection_ref)
        assert isinstance(target_connection, ConnectionBase)

        prep_src_info = PreparedSingleFromInfo(
            id=avatar_id,
            alias=alias,
            data_source_list=(dsrc,),
            col_names=col_names,
            user_types=[col.user_type for col in columns],
            sql_source=sql_source,
            query_compiler=query_compiler,
            supported_join_types=dsrc.supported_join_types,
            db_name=(dsrc.db_name if isinstance(dsrc, bi_core.data_source.sql.DbSQLDataSourceMixin) else None),
            connect_args=dsrc.get_connect_args(),
            pass_db_query_to_user=target_connection.pass_db_query_to_user,
            target_connection_ref=target_connection_ref,
        )
        return prep_src_info

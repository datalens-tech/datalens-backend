from __future__ import annotations

from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
)

from clickhouse_sqlalchemy.quoting import Quoter

from dl_constants.enums import (
    DataSourceType,
    FileProcessingStatus,
)
from dl_core import exc
from dl_core.db import SchemaInfo
from dl_core.query.bi_query import SqlSourceType
from dl_core.utils import sa_plain_text

from dl_connector_bundle_chs3.chs3_base.core.data_source_spec import BaseFileS3DataSourceSpec
from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.file.core.adapter import AsyncFileS3Adapter
from dl_connector_clickhouse.core.clickhouse_base.ch_commons import create_column_sql
from dl_connector_clickhouse.core.clickhouse_base.data_source import ClickHouseDataSourceBase


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


def require_file_configured(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(self: BaseFileS3DataSource, *args: Any, **kwargs: Any) -> Any:
        if self._get_origin_src().status != FileProcessingStatus.ready:
            raise exc.TableNameNotConfiguredError

        return func(self, *args, **kwargs)

    return wrapper


class BaseFileS3DataSource(ClickHouseDataSourceBase):
    preview_enabled: ClassVar[bool] = True
    store_raw_schema = True
    _quoter: Optional[Quoter] = None

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        raise NotImplementedError

    @property
    def origin_source_id(self) -> Optional[str]:
        return self.spec.origin_source_id

    def _get_origin_src(self) -> BaseFileS3Connection.FileDataSource:
        return self.connection.get_file_source_by_id(self.origin_source_id)

    @property
    def connection(self) -> BaseFileS3Connection:
        connection = super().connection
        assert isinstance(connection, BaseFileS3Connection)
        return connection

    @require_file_configured
    def get_schema_info(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> SchemaInfo:
        origin_src = self._get_origin_src()
        assert origin_src.raw_schema is not None
        source_schema = origin_src.raw_schema
        return SchemaInfo.from_schema(source_schema)

    @require_file_configured
    def source_exists(
        self,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
        force_refresh: bool = False,
    ) -> bool:
        try:
            self.connection.get_file_source_by_id(self.origin_source_id)
        except exc.SourceDoesNotExist:
            return False
        return True

    @property
    def s3_endpoint(self) -> Optional[str]:
        return self.spec.s3_endpoint

    @property
    def bucket(self) -> Optional[str]:
        return self.spec.bucket

    @property
    def status(self) -> FileProcessingStatus:
        return self.spec.status

    @property
    def spec(self) -> BaseFileS3DataSourceSpec:
        assert isinstance(self._spec, BaseFileS3DataSourceSpec)
        return self._spec

    def get_parameters(self) -> dict:
        return dict(
            origin_source_id=self.origin_source_id,
        )

    def quote_str(self, value: str) -> str:
        if self._quoter is None:
            self._quoter = Quoter()
        return self._quoter.quote_str(value)

    def _handle_component_errors(self) -> None:
        pass

    def get_sql_source(self, alias: Optional[str] = None) -> SqlSourceType:
        origin_src = self._get_origin_src()
        status = origin_src.status
        raw_schema = self.spec.raw_schema

        s3_filename: str | None
        if origin_src.s3_filename_suffix is not None:
            s3_filename = self.connection.get_full_s3_filename(origin_src.s3_filename_suffix)
        else:
            # TODO: Remove this fallback after old connections migration to s3_filename_suffix
            s3_filename = origin_src.s3_filename

        self._handle_component_errors()

        if status != FileProcessingStatus.ready or raw_schema is None or s3_filename is None:
            raise exc.MaterializationNotFinished

        replace_secret = self.connection.get_replace_secret()

        s3_endpoint = self.connection.s3_endpoint
        bucket = self.connection.bucket
        assert s3_endpoint is not None and bucket is not None and raw_schema is not None

        s3_path = self.quote_str("{}/{}/{}".format(s3_endpoint.strip("/"), bucket.strip("/"), s3_filename))
        key_id_placeholder = self.quote_str(f"key_id_{replace_secret}")
        secret_key_placeholder = self.quote_str(f"secret_key_{replace_secret}")
        file_fmt = self.quote_str("Native")
        dialect = AsyncFileS3Adapter.get_dialect()
        schema_str = self.quote_str(
            ", ".join(create_column_sql(dialect, col, self.type_transformer) for col in raw_schema)
        )
        alias_str = "" if alias is None else f" AS {self.quote(alias)}"

        return sa_plain_text(
            f"s3({s3_path}, {key_id_placeholder}, {secret_key_placeholder}, {file_fmt}, {schema_str}){alias_str}"
        )

from __future__ import annotations

from collections import defaultdict
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
)

import attr
import xxhash

from dl_constants.enums import (
    DataSourceRole,
    FileProcessingStatus,
)
from dl_core import (
    connection_models,
    exc,
)
from dl_core.base_models import ConnectionDataModelBase
from dl_core.component_errors import ComponentErrorRegistry
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.db.elements import SchemaColumn
from dl_core.services_registry.file_uploader_client_factory import FileSourceDesc
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionHardcodedDataMixin,
    DataSourceTemplate,
)
from dl_core.utils import (
    make_user_auth_cookies,
    make_user_auth_headers,
    parse_comma_separated_hosts,
)

from dl_connector_bundle_chs3.chs3_base.core.dto import BaseFileS3ConnDTO
from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettings
from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from dl_connector_clickhouse.core.clickhouse_base.us_connection import ConnectionClickhouseBase


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class BaseFileS3Connection(ConnectionHardcodedDataMixin[FileS3ConnectorSettings], ConnectionClickhouseBase):
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    settings_type = FileS3ConnectorSettings
    allow_export: ClassVar[bool] = True

    editable_data_source_parameters: ClassVar[tuple[str, ...]] = (
        "file_id",
        "title",
        "s3_filename",
        "s3_filename_suffix",
        "status",
        "preview_id",
    )

    @attr.s(kw_only=True)
    class FileDataSource:
        id: str = attr.ib()
        file_id: str = attr.ib()
        title: str = attr.ib()
        preview_id: Optional[str] = attr.ib(default=None)
        status: FileProcessingStatus = attr.ib(default=FileProcessingStatus.in_progress)
        s3_filename: Optional[str] = attr.ib(default=None)
        s3_filename_suffix: Optional[str] = attr.ib(default=None)
        raw_schema: Optional[list[SchemaColumn]] = attr.ib(factory=list[SchemaColumn])

        def str_for_hash(self) -> str:
            return ",".join(
                [
                    self.id,
                    self.file_id,
                    self.title,
                    str(self.s3_filename),
                    str(self.s3_filename_suffix),
                    self.status.name,
                ]
            )

        def get_desc(self) -> FileSourceDesc:
            raise NotImplementedError()

    @attr.s(eq=False, kw_only=True)
    class DataModel(ConnectionDataModelBase):
        sources: list["BaseFileS3Connection.FileDataSource"] = attr.ib()
        replace_sources: list[dict] = attr.ib(factory=list)  # should not be saved in US
        data_export_forbidden: bool = attr.ib(default=False)

        component_errors: ComponentErrorRegistry = attr.ib(factory=ComponentErrorRegistry)

        def str_for_hash(self) -> str:
            return "|".join(src.str_for_hash() for src in self.sources)

    data: DataModel

    def get_replace_secret(self) -> str:
        return xxhash.xxh64(self.data.str_for_hash() + self._connector_settings.REPLACE_SECRET_SALT, seed=0).hexdigest()

    @property
    def bucket(self) -> str:
        return self._connector_settings.BUCKET

    @property
    def s3_endpoint(self) -> str:
        return self._connector_settings.S3_ENDPOINT

    @property
    def s3_access_key_id(self) -> str:
        return self._connector_settings.ACCESS_KEY_ID

    @property
    def s3_secret_access_key(self) -> str:
        return self._connector_settings.SECRET_ACCESS_KEY

    def get_full_s3_filename(self, s3_filename_suffix: str) -> str:
        assert self.uuid and self.raw_tenant_id
        return "_".join((self.raw_tenant_id, self.uuid, s3_filename_suffix))

    def get_conn_dto(self) -> BaseFileS3ConnDTO:
        cs = self._connector_settings
        conn_dto = BaseFileS3ConnDTO(
            conn_id=self.uuid,
            s3_endpoint=cs.S3_ENDPOINT,
            access_key_id=cs.ACCESS_KEY_ID,
            secret_access_key=cs.SECRET_ACCESS_KEY,
            bucket=cs.BUCKET,
            replace_secret=self.get_replace_secret(),
            protocol="https" if cs.SECURE else "http",
            host=cs.HOST,
            multihosts=parse_comma_separated_hosts(cs.HOST),
            port=cs.PORT,
            username=cs.USERNAME,
            password=cs.PASSWORD,
        )
        return conn_dto

    def get_conn_options(self) -> CHConnectOptions:
        return self.get_effective_conn_options(
            base_conn_opts=connection_models.ConnectOptions(pass_db_messages_to_user=False),
            max_allowed_max_execution_time=self.MAX_ALLOWED_MAX_EXECUTION_TIME,
            user_max_execution_time=None,
        )

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return 600

    def get_data_source_templates(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[DataSourceTemplate]:
        assert self.source_type is not None
        return [
            DataSourceTemplate(
                title=source.title,
                group=[],
                source_type=self.source_type,
                connection_id=self.uuid,  # type: ignore  # 2024-01-30 # TODO: Argument "connection_id" to "DataSourceTemplate" has incompatible type "str | None"; expected "str"  [arg-type]
                parameters={
                    "origin_source_id": source.id,
                },
            )
            for source in self.data.sources
        ]

    def get_file_source_by_id(self, id: Optional[str]) -> FileDataSource:
        file_source = next(iter(src for src in self.data.sources if src.id == id), None)
        if file_source is None:
            raise exc.SourceDoesNotExist(f"DataSource id={id} not found in connection id={self.uuid}")
        return file_source

    def remove_source_by_id(self, id: str) -> None:
        idx = next(iter(i for i, src in enumerate(self.data.sources) if src.id == id), None)
        if idx is not None:
            del self.data.sources[idx]
        else:
            LOGGER.warning(f"DataSource id={id} not found in connection id={self.uuid} when trying to remove it")

    def _remove_saved_source_by_id(self, id: str) -> None:
        assert self._saved_sources is not None
        idx = next(iter(i for i, src in enumerate(self._saved_sources) if src.id == id), None)
        if idx is not None:
            del self._saved_sources[idx]
        else:
            LOGGER.warning(
                f"DataSource id={id} not found in connection id={self.uuid} saved sources when trying to remove it"
            )

    def get_saved_source_by_id(self, id: Optional[str]) -> FileDataSource:
        file_source = next(iter(src for src in self._saved_sources or [] if src.id == id), None)
        if file_source is None:
            raise exc.SourceDoesNotExist(f"DataSource id={id} not found in saved sources of connection id={self.uuid}")
        return file_source

    def update_data_source(
        self,
        id: str,
        role: Optional[DataSourceRole] = None,
        raw_schema: Optional[list] = None,
        remove_raw_schema: bool = False,
        **parameters: Any,
    ) -> None:
        if role != DataSourceRole.origin:
            raise ValueError(f"Unsupported role for {self.__class__.__name__} datasource.")

        source = self.get_file_source_by_id(id)

        sentinel = object()
        filtered_parameters = {
            param_name: param_value
            for param_name in self.editable_data_source_parameters
            if (param_value := parameters.pop(param_name, sentinel)) is not sentinel
        }
        if parameters:
            raise ValueError(f"Unknown update_data_source parameter: {parameters}")

        LOGGER.info(f"Updating data source {id} with role {role}. Parameters: {filtered_parameters}")

        if raw_schema is not None:
            source.raw_schema = raw_schema
            LOGGER.info("Updated data source raw_schema")
        if remove_raw_schema:
            source.raw_schema = None
            LOGGER.info("Removed data source raw_schema")

        for param_name, value in filtered_parameters.items():
            setattr(source, param_name, value)

    def replace_sources(self) -> None:
        assert self._saved_sources is not None
        for upd_source in self.data.replace_sources:
            old_src = self.get_saved_source_by_id(upd_source["old_source_id"])
            new_src = self.get_file_source_by_id(upd_source["new_source_id"])
            new_src.id, old_src.id = old_src.id, new_src.id

    def restore_source_params_from_orig(self, src_id: str, original_version: BaseFileS3Connection) -> None:
        raise NotImplementedError()

    def test(self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase]) -> None:
        """
        Don't execute `select 1` on our service databases - it's useless because user can't
        manage it anyway.
        """
        pass

    def validate(self) -> None:
        try:
            pass  # TODO
        except exc.EntryValidationError as err:
            raise err

    _saved_sources: Optional[list[FileDataSource]] = None

    async def validate_new_data(
        self,
        services_registry: ServicesRegistry,
        changes: Optional[dict] = None,
        original_version: Optional[ConnectionBase] = None,
    ) -> None:
        assert isinstance(original_version, (type(self), type(None)))
        if original_version is None:
            saved_sources = set()
        else:
            saved_sources = set(src.id for src in original_version.data.sources)

        if changes is None:
            sources_to_update = set()
        else:
            # save source if we receive file_id regardless of whether it was saved or not
            sources_to_update = set(src.id for src in changes["data"]["sources"] if src.file_id is not None)

        current_sources = set(src.id for src in self.data.sources)
        sources_to_add = (current_sources - saved_sources) | sources_to_update
        unaltered_sources = current_sources & saved_sources - sources_to_update

        if len(current_sources) != len(set(src.title for src in self.data.sources)):
            raise exc.DataSourceTitleConflict

        err_details = defaultdict(list)
        for src in self.data.sources:
            if src.file_id is None and src.id not in saved_sources:
                err_details["not_configured_not_saved"].append(src.id)

        for replace_src in self.data.replace_sources:
            if replace_src["old_source_id"] not in saved_sources:
                err_details["replaced_not_saved"].append(replace_src["old_source_id"])

        if err_details:
            raise exc.DataSourcesInconsistent(details=err_details)

        if original_version is not None:
            for src_id in unaltered_sources:
                # Restore original internal source properties
                self.restore_source_params_from_orig(src_id, original_version)

        rci = self.us_manager.bi_context
        headers = make_user_auth_headers(rci=rci)
        cookies = make_user_auth_cookies(rci=rci)
        fu_client_factory = self.us_manager.get_services_registry().get_file_uploader_client_factory()

        sources_desc = []
        for src_id in sources_to_add:
            src = self.get_file_source_by_id(src_id)
            sources_desc.append(src.get_desc())

        if sources_desc:
            async with fu_client_factory.get_client(headers=headers, cookies=cookies) as fu_client:
                internal_params = await fu_client.get_internal_params_batch(sources_desc)

            for src_order, src_id in enumerate(sources_to_add):
                self.update_data_source(
                    src_id,
                    role=DataSourceRole.origin,
                    raw_schema=internal_params[src_order].raw_schema,
                    preview_id=internal_params[src_order].preview_id,
                )

        if self._saved_sources is not None:
            saved_source_ids = set(src.id for src in self._saved_sources)
            for src_id in sources_to_update & saved_source_ids:  # trigger update by removing source from _saved_sources
                self._remove_saved_source_by_id(src_id)

        # clear errors for updated sources
        for src_id in sources_to_add:
            self.data.component_errors.remove_errors(id=src_id)

        if self.data.replace_sources:
            self.replace_sources()

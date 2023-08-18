from __future__ import annotations

import logging
from typing import Any, Optional, ClassVar

from aiohttp import web

from bi_constants.enums import BIType, ConnectionType, FileProcessingStatus
from bi_api_commons.aiohttp.aiohttp_wrappers import RequiredResource, RequiredResourceCommon
from bi_core.db import get_type_transformer
from bi_core.db.elements import SchemaColumn
from bi_file_uploader_task_interface.tasks import ParseFileTask

from bi_file_uploader_lib import exc
from bi_file_uploader_lib.enums import FileType, CSVEncoding, CSVDelimiter
from bi_file_uploader_lib.redis_model.base import RedisModelManager
from bi_file_uploader_lib.redis_model.models.models import (
    DataFile,
    DataSourcePreview,
    CSVFileSettings,
    DataSource,
    FileSettings,
    UserSourceProperties,
    GSheetsUserSourceProperties,
    GSheetsUserSourceDataSourceProperties,
)

from bi_file_uploader_api_lib.views.base import FileUploaderBaseView
from bi_file_uploader_api_lib.schemas import sources as sources_schemas


LOGGER = logging.getLogger(__name__)


RawSchemaType = list[SchemaColumn]


class SourceStatusView(FileUploaderBaseView):
    async def get(self) -> web.StreamResponse:
        req_data = sources_schemas.SourceInfoBaseRequestSchema().load(self.request.match_info)
        rmm = self.dl_request.get_redis_model_manager()
        dfile = await DataFile.get_authorized(manager=rmm, obj_id=req_data['file_id'])
        assert isinstance(dfile, DataFile)
        source = dfile.get_source_by_id(req_data['source_id'])

        if dfile.status == FileProcessingStatus.failed:
            # TODO: temporary hack. Should be fixed with properly parsing errors saving.
            status = dfile.status
        else:
            status = source.status

        return web.json_response(
            sources_schemas.SourceStatusResultSchema().dump({
                'file_id': dfile.id,
                'source_id': source.id,
                'status': status,
                'error': source.error,
            }),
        )


def get_compatible_user_type(user_type: Optional[BIType]) -> tuple[BIType, ...]:
    compatible_types: dict[Optional[BIType], tuple[BIType, ...]] = {
        BIType.string: (BIType.string,),
        BIType.integer: (BIType.integer, BIType.float, BIType.string,),
        BIType.float: (BIType.float, BIType.string,),
        BIType.date: (BIType.date, BIType.genericdatetime, BIType.string,),
        BIType.genericdatetime: (BIType.genericdatetime, BIType.string,),
    }
    return compatible_types.get(user_type, tuple())


def cast_preview_data(
        preview_data: list[list[Optional[str]]],
        raw_schema: RawSchemaType,
) -> list[list[Optional[str]]]:
    tt = get_type_transformer(ConnectionType.file)
    for row in preview_data:
        for i, cell in enumerate(row):
            cast_value = tt.cast_for_input(cell, raw_schema[i].user_type)
            row[i] = str(cast_value) if cast_value is not None else None
    return preview_data


def get_raw_schema_with_overrides(raw_schema: RawSchemaType, schema_overrides: RawSchemaType) -> RawSchemaType:
    orig_column_types_by_name: dict[str, BIType] = {sch.name: sch.user_type for sch in raw_schema}
    column_type_overrides = dict()
    for col in schema_overrides:
        col_name = col.name
        new_type = col.user_type
        orig_type = orig_column_types_by_name.get(col_name)
        if new_type not in get_compatible_user_type(orig_type):
            raise exc.InvalidFieldCast(f'Unsupported cast {orig_type} to {new_type} for field {col_name}')
        column_type_overrides[col_name] = new_type

    raw_schema_with_overrides: RawSchemaType = [
        col.clone(user_type=column_type_overrides[col.name])
        if col.name in column_type_overrides
        else col
        for col in raw_schema
    ]
    return raw_schema_with_overrides


class SourceInfoView(FileUploaderBaseView):
    async def _make_source_resp(
            self,
            file_type: FileType,
            user_source_properties: Optional[UserSourceProperties],
            source: DataSource,
            column_type_overrides: RawSchemaType,
            rmm: RedisModelManager,
    ) -> dict[Any, Any]:
        preview_id = source.preview_id
        if preview_id is not None:
            preview = await DataSourcePreview.get(manager=rmm, obj_id=preview_id)
            assert isinstance(preview, DataSourcePreview)
        else:
            preview = DataSourcePreview(preview_data=[])

        raw_schema = get_raw_schema_with_overrides(source.raw_schema, column_type_overrides)

        extra: dict[str, Any] = {}
        if file_type == FileType.gsheets:
            assert isinstance(user_source_properties, GSheetsUserSourceProperties)
            extra['spreadsheet_id'] = user_source_properties.spreadsheet_id
            assert isinstance(source.user_source_dsrc_properties, GSheetsUserSourceDataSourceProperties)
            extra['sheet_id'] = source.user_source_dsrc_properties.sheet_id

        source_resp = dict(
            file_type=file_type,
            source_id=source.id,
            title=source.title,
            is_valid=True,  # TODO
            raw_schema=raw_schema,
            preview=cast_preview_data(preview.preview_data, raw_schema),
            error=source.error,
            **extra,
        )

        return source_resp

    def _make_data_settings(self, source: DataSource, file_settings: Optional[FileSettings]) -> dict[str, Any]:
        data_settings: dict[str, Any] = dict()
        if source.file_source_settings is not None:
            data_settings['first_line_is_header'] = source.file_source_settings.first_line_is_header
        if isinstance(file_settings, CSVFileSettings):
            data_settings['encoding'] = file_settings.encoding
            data_settings['delimiter'] = file_settings.delimiter
        return data_settings

    def _make_options(self, file_type: FileType, source: DataSource) -> Optional[dict[str, Any]]:
        if file_type == FileType.csv:
            return dict(
                data_settings={
                    'encoding': sorted((enc for enc in CSVEncoding), key=lambda t: t.name),
                    'delimiter': sorted((delim for delim in CSVDelimiter), key=lambda t: t.name),
                },
                columns=[
                    {'name': sch.name, 'user_type': get_compatible_user_type(sch.user_type)}
                    for sch in source.raw_schema
                ],
            )
        return None

    async def _prepare_source_info(self, req_data: dict[str, Any]) -> web.StreamResponse:
        rmm = self.dl_request.get_redis_model_manager()
        dfile = await DataFile.get_authorized(manager=rmm, obj_id=req_data['file_id'])
        assert isinstance(dfile, DataFile)
        source = dfile.get_source_by_id(req_data['source_id'])

        # TODO: check if source.status == ready ???

        assert dfile.file_type is not None
        source_resp = await self._make_source_resp(
            file_type=dfile.file_type,
            user_source_properties=dfile.user_source_properties,
            source=source,
            column_type_overrides=[] if dfile.file_type == FileType.gsheets else req_data['column_types'],
            rmm=rmm,
        )

        assert dfile.file_type is not None
        return web.json_response(
            sources_schemas.SourceInfoResultSchema().dump(dict(
                file_id=dfile.id,
                source=source_resp,
                data_settings=self._make_data_settings(source, dfile.file_settings),
                options=self._make_options(dfile.file_type, source),
            )),
        )

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(sources_schemas.SourceInfoRequestSchema)
        return await self._prepare_source_info(req_data)


class SourceApplySettingsView(FileUploaderBaseView):
    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(sources_schemas.SourceApplySettingsRequestSchema)
        rmm = self.dl_request.get_redis_model_manager()
        dfile = await DataFile.get_authorized(manager=rmm, obj_id=req_data['file_id'])
        assert isinstance(dfile, DataFile)
        source = dfile.get_source_by_id(req_data['source_id'])
        LOGGER.info(f'Applying data settings for file {dfile.id}, source {source.id}')
        dfile.status = FileProcessingStatus.in_progress
        source.status = FileProcessingStatus.in_progress
        if req_data['title'] is not None:
            source.title = req_data['title']
        LOGGER.info('Switching file and source status to `in_progress`')
        await dfile.save()

        LOGGER.info(f'Settings to apply: {req_data["data_settings"]}')

        task_processor = self.dl_request.get_task_processor()
        await task_processor.schedule(ParseFileTask(
            file_id=dfile.id,
            source_id=source.id,
            file_settings=sources_schemas.CSVSettingsSchema().dump(req_data["data_settings"]),
            source_settings={},
        ))
        LOGGER.info(f'Scheduled ParseFileTask for file {dfile.id}, source {source.id}')
        return web.Response()


class SourcePreviewView(FileUploaderBaseView):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset({
        RequiredResourceCommon.SKIP_CSRF,
        RequiredResourceCommon.MASTER_KEY,
    })

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(sources_schemas.SourcePreviewRequestSchema)
        rmm = self.dl_request.get_redis_model_manager()

        preview = await DataSourcePreview.get(manager=rmm, obj_id=req_data['preview_id'])
        assert isinstance(preview, DataSourcePreview)

        return web.json_response(
            sources_schemas.SourcePreviewResultSchema().dump(dict(
                preview=cast_preview_data(preview.preview_data, req_data['raw_schema']),
            )),
        )


class SourceInternalParamsView(FileUploaderBaseView):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset({
        RequiredResourceCommon.SKIP_CSRF,
        RequiredResourceCommon.MASTER_KEY,
    })

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(sources_schemas.SourceInternalParamsRequestSchema)
        rmm = self.dl_request.get_redis_model_manager()
        dfile = await DataFile.get_authorized(manager=rmm, obj_id=req_data['file_id'])
        assert isinstance(dfile, DataFile)
        source = dfile.get_source_by_id(req_data['source_id'])
        preview_id = source.preview_id

        column_type_overrides: Optional[RawSchemaType] = req_data['raw_schema']
        LOGGER.info(f'Got overrides for raw_schema: {column_type_overrides=}')
        if column_type_overrides is not None:
            raw_schema = get_raw_schema_with_overrides(source.raw_schema, column_type_overrides)
        else:
            raw_schema = source.raw_schema
        LOGGER.info(f"Raw schema for file id {req_data['file_id']}, source id {req_data['source_id']}: {raw_schema}")

        return web.json_response(
            sources_schemas.SourceInternalParamsResultSchema().dump(dict(raw_schema=raw_schema, preview_id=preview_id)),
        )

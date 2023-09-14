from __future__ import annotations

from http import HTTPStatus
import logging
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
    Optional,
)

from aiohttp import web
from aiohttp.multipart import BodyPartReader
from redis.asyncio.lock import Lock as RedisLock

from bi_api_commons.aiohttp.aiohttp_wrappers import (
    RequiredResource,
    RequiredResourceCommon,
)
from bi_constants.enums import FileProcessingStatus
from bi_file_uploader_api_lib.data_file_preparer import gsheets_data_file_preparer
from bi_file_uploader_api_lib.schemas import files as files_schemas
from bi_file_uploader_api_lib.views.base import FileUploaderBaseView
from bi_file_uploader_lib.common_locks import get_update_connection_source_lock
from bi_file_uploader_lib.data_sink.raw_bytes import (
    RawBytesAsyncDataStream,
    S3RawFileAsyncDataSink,
)
from bi_file_uploader_lib.enums import FileType
from bi_file_uploader_lib.redis_model.base import RedisModelManager
from bi_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSource,
    GSheetsFileSourceSettings,
    GSheetsUserSourceDataSourceProperties,
    GSheetsUserSourceProperties,
)
from bi_file_uploader_task_interface.tasks import (
    DownloadGSheetTask,
    ParseFileTask,
    ProcessExcelTask,
    TaskExecutionMode,
)

LOGGER = logging.getLogger(__name__)


def get_file_type_from_name(
    filename: Optional[str],
    default_type: FileType = FileType.csv,
    allow_xlsx: bool = False,
) -> FileType:
    FILE_TYPE_MAPPER = {"csv": FileType.csv}
    if allow_xlsx:
        FILE_TYPE_MAPPER["xlsx"] = FileType.xlsx
    file_extension = "csv"
    if isinstance(filename, str) and "." in filename:
        file_extension = filename.split(".")[-1]
    file_type = FILE_TYPE_MAPPER.get(file_extension, default_type)
    return file_type


class FilesView(FileUploaderBaseView):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset()  # Don't skip CSRF check

    async def post(self) -> web.StreamResponse:
        reader = await self.request.multipart()
        field = await reader.next()
        assert isinstance(field, BodyPartReader)
        assert field.name == "file"
        filename = field.filename
        file_type = get_file_type_from_name(filename=filename, allow_xlsx=self.request.app["ALLOW_XLSX"])

        s3 = self.dl_request.get_s3_service()

        rmm = self.dl_request.get_redis_model_manager()
        df = DataFile(
            manager=rmm,
            filename=filename,
            file_type=file_type,
            status=FileProcessingStatus.in_progress,
        )
        LOGGER.info(f"Data file id: {df.id}")

        async def _chunk_iter(chunk_size: int = 10 * 1024 * 1024) -> AsyncGenerator[bytes, None]:
            assert isinstance(field, BodyPartReader)
            while True:
                chunk = await field.read_chunk(size=chunk_size)
                if chunk:
                    LOGGER.debug(f"Received chunk of {len(chunk)} bytes.")
                    yield chunk
                else:
                    LOGGER.info("Empty chunk received.")
                    break

        data_stream = RawBytesAsyncDataStream(data_iter=_chunk_iter())
        async with S3RawFileAsyncDataSink(
            s3=s3.client,
            s3_key=df.s3_key,
            bucket_name=s3.tmp_bucket_name,
        ) as data_sink:
            await data_sink.dump_data_stream(data_stream)

        # df.size = size    # TODO

        await df.save()
        LOGGER.info(f'Uploaded file "{filename}".')

        task_processor = self.dl_request.get_task_processor()
        if file_type == FileType.xlsx:
            await task_processor.schedule(ProcessExcelTask(file_id=df.id))
            LOGGER.info(f"Scheduled ProcessExcelTask for file_id {df.id}")
        else:
            await task_processor.schedule(ParseFileTask(file_id=df.id))
            LOGGER.info(f"Scheduled ParseFileTask for file_id {df.id}")

        return web.json_response(
            files_schemas.FileUploadResponseSchema().dump({"file_id": df.id, "title": df.filename}),
            status=HTTPStatus.CREATED,
        )


class LinksView(FileUploaderBaseView):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset()  # Don't skip CSRF check

    FILE_TYPE_TO_DATA_FILE_PREPARER_MAP: dict[
        FileType, Callable[[str, RedisModelManager, Optional[str]], Awaitable[DataFile]]
    ] = {
        FileType.gsheets: gsheets_data_file_preparer,
    }

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(files_schemas.FileLinkRequestSchema)

        file_type = FileType(req_data["type"])

        rmm = self.dl_request.get_redis_model_manager()

        refresh_token: Optional[str] = req_data["refresh_token"]
        connection_id: Optional[str] = req_data["connection_id"]
        authorized: bool = req_data["authorized"]
        df = await self.FILE_TYPE_TO_DATA_FILE_PREPARER_MAP[file_type](req_data["url"], rmm, refresh_token)

        LOGGER.info(f"Data file id: {df.id}")
        await df.save()

        task_processor = self.dl_request.get_task_processor()
        await task_processor.schedule(
            DownloadGSheetTask(
                file_id=df.id,
                authorized=authorized,
                connection_id=connection_id,
            )
        )
        LOGGER.info(f"Scheduled DownloadGSheetTask for file_id {df.id}")

        return web.json_response(
            files_schemas.FileUploadResponseSchema().dump({"file_id": df.id, "title": df.filename}),
            status=HTTPStatus.CREATED,
        )


class FileStatusView(FileUploaderBaseView):
    async def get(self) -> web.StreamResponse:
        req_data = files_schemas.FileStatusRequestSchema().load(self.request.match_info)
        rmm = self.dl_request.get_redis_model_manager()
        df = await DataFile.get_authorized(manager=rmm, obj_id=req_data["file_id"])

        return web.json_response(files_schemas.FileStatusResultSchema().dump(df))


class FileSourcesView(FileUploaderBaseView):
    async def get(self) -> web.StreamResponse:
        req_data = files_schemas.FileSourcesRequestSchema().load(self.request.match_info)
        rmm = self.dl_request.get_redis_model_manager()
        df = await DataFile.get_authorized(manager=rmm, obj_id=req_data["file_id"])

        return web.json_response(files_schemas.FileSourcesResultSchema().dump(df))


class UpdateConnectionDataView(FileUploaderBaseView):
    _is_internal: ClassVar[bool] = False

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(files_schemas.UpdateConnectionDataRequestSchema)
        sources = req_data["sources"]

        rmm = self.dl_request.get_redis_model_manager()
        redis = self.dl_request.get_persistent_redis()

        dfile_by_spreadsheet: dict[str, DataFile] = {}
        for src in sources:
            source_lock_key, source_lock_token = get_update_connection_source_lock(src["id"])
            LOGGER.info(f"Acquiring redis lock {source_lock_key}")
            # source locks will be released by file uploader worker when the update process is finished
            source_lock = RedisLock(redis, name=source_lock_key, timeout=120, blocking=False)
            if not await source_lock.acquire(token=source_lock_token):
                LOGGER.warning(f"Cannot acquire lock {source_lock_key}, skipping the source")
                continue
            LOGGER.info(f"Lock {source_lock_key} acquired")

            if src["spreadsheet_id"] not in dfile_by_spreadsheet:
                dfile_by_spreadsheet[src["spreadsheet_id"]] = DataFile(
                    user_id=DataFile.system_user_id if self._is_internal else None,  # filled from rci when None
                    manager=rmm,
                    filename="TITLE",
                    status=FileProcessingStatus.in_progress,
                    file_type=FileType.gsheets,
                    sources=[],
                    user_source_properties=GSheetsUserSourceProperties(
                        spreadsheet_id=src["spreadsheet_id"],
                        refresh_token=req_data["refresh_token"],
                    ),
                )
            LOGGER.info(f'Data file id: {dfile_by_spreadsheet[src["spreadsheet_id"]].id}')

            sheet_data_source = DataSource(
                id=src["id"],
                title=src["title"],
                raw_schema=[],
                file_source_settings=GSheetsFileSourceSettings(
                    first_line_is_header=src["first_line_is_header"],
                    raw_schema_header=[],
                    raw_schema_body=[],
                ),
                user_source_dsrc_properties=GSheetsUserSourceDataSourceProperties(
                    sheet_id=src["sheet_id"],
                ),
                status=FileProcessingStatus.in_progress,
                error=None,
            )

            dfile_by_spreadsheet[src["spreadsheet_id"]].sources.append(sheet_data_source)  # type: ignore

        task_processor = self.dl_request.get_task_processor()
        exec_mode = TaskExecutionMode.UPDATE_AND_SAVE if req_data["save"] else TaskExecutionMode.UPDATE_NO_SAVE
        if req_data.get("tenant_id") is not None:
            tenant_id = req_data["tenant_id"]
        else:
            assert self.dl_request.rci.tenant is not None
            tenant_id = self.dl_request.rci.tenant.get_tenant_id()
        assert tenant_id is not None
        for dfile in dfile_by_spreadsheet.values():
            await dfile.save()

            download_gsheet_task = DownloadGSheetTask(
                file_id=dfile.id,
                authorized=req_data["authorized"],
                tenant_id=tenant_id,
                connection_id=req_data["connection_id"],
                exec_mode=exec_mode,
            )
            await task_processor.schedule(download_gsheet_task)
            LOGGER.info(f"Scheduled DownloadGSheetTask for file_id {dfile.id} (update connection)")

        return web.json_response(
            files_schemas.UpdateConnectionDataResultSchema().dump(dict(files=dfile_by_spreadsheet.values()))
        )


class InternalUpdateConnectionDataView(UpdateConnectionDataView):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset(
        {
            RequiredResourceCommon.SKIP_AUTH,
            RequiredResourceCommon.SKIP_CSRF,
            RequiredResourceCommon.MASTER_KEY,
        }
    )

    _is_internal: ClassVar[bool] = True

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
import uuid

from aiohttp import web
from aiohttp.multipart import BodyPartReader
from redis.asyncio.lock import Lock as RedisLock

from dl_api_commons.aiohttp.aiohttp_wrappers import (
    RequiredResource,
    RequiredResourceCommon,
)
from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_api_lib.data_file_preparer import (
    gsheets_data_file_preparer,
    yadocs_data_file_preparer,
)
from dl_file_uploader_api_lib.schemas import files as files_schemas
from dl_file_uploader_api_lib.views.base import FileUploaderBaseView
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.common_locks import get_update_connection_source_lock
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSource,
    GSheetsFileSourceSettings,
    GSheetsUserSourceDataSourceProperties,
    GSheetsUserSourceProperties,
    YaDocsFileSourceSettings,
    YaDocsUserSourceDataSourceProperties,
    YaDocsUserSourceProperties,
)
from dl_file_uploader_task_interface.tasks import (
    DownloadGSheetTask,
    DownloadYaDocsTask,
    ParseFileTask,
    ProcessExcelTask,
    TaskExecutionMode,
)
from dl_s3.data_sink import S3RawFileAsyncDataSink
from dl_s3.stream import RawBytesAsyncDataStream
from dl_s3.utils import s3_file_exists


LOGGER = logging.getLogger(__name__)

S3_KEY_PARTS_SEPARATOR = "/"  # used to separate author user_id from the rest of the s3 object key to sign it


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
        dfile = DataFile(
            manager=rmm,
            filename=filename,
            file_type=file_type,
            status=FileProcessingStatus.in_progress,
        )
        LOGGER.info(f"Data file id: {dfile.id}")

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
            s3_key=dfile.s3_key_old,
            bucket_name=s3.tmp_bucket_name,
            max_file_size_exc=exc.FileLimitError,
        ) as data_sink:
            await data_sink.dump_data_stream(data_stream)

        # df.size = size    # TODO

        await dfile.save()
        LOGGER.info(f'Uploaded file "{filename}".')

        task_processor = self.dl_request.get_task_processor()
        if file_type == FileType.xlsx:
            await task_processor.schedule(ProcessExcelTask(file_id=dfile.id))
            LOGGER.info(f"Scheduled ProcessExcelTask for file_id {dfile.id}")
        else:
            await task_processor.schedule(ParseFileTask(file_id=dfile.id))
            LOGGER.info(f"Scheduled ParseFileTask for file_id {dfile.id}")

        return web.json_response(
            files_schemas.FileUploadResponseSchema().dump({"file_id": dfile.id, "title": dfile.filename}),
            status=HTTPStatus.CREATED,
        )


class MakePresignedUrlView(FileUploaderBaseView):
    PRESIGNED_URL_EXPIRATION_SECONDS: ClassVar[int] = 60 * 60  # 1 hour
    PRESIGNED_URL_MIN_BYTES: ClassVar[int] = 1
    PRESIGNED_URL_MAX_BYTES: ClassVar[int] = 200 * 1024**2  # 200 MB

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(files_schemas.MakePresignedUrlRequestSchema)
        content_md5: str = req_data["content_md5"]

        s3 = self.dl_request.get_s3_service()
        s3_key = S3_KEY_PARTS_SEPARATOR.join(
            (
                self.dl_request.rci.user_id or "unknown",
                str(uuid.uuid4()),
            )
        )

        url = await s3.client.generate_presigned_post(
            Bucket=s3.tmp_bucket_name,
            Key=s3_key,
            ExpiresIn=self.PRESIGNED_URL_EXPIRATION_SECONDS,
            Conditions=[
                ["content-length-range", self.PRESIGNED_URL_MIN_BYTES, self.PRESIGNED_URL_MAX_BYTES],
                {"Content-MD5": content_md5},
            ],
        )

        return web.json_response(
            files_schemas.PresignedUrlSchema().dump(url),
            status=HTTPStatus.OK,
        )


class DownloadPresignedUrlView(FileUploaderBaseView):
    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(files_schemas.DownloadPresignedUrlRequestSchema)
        filename: str = req_data["filename"]
        s3_key: str = req_data["key"]

        file_type = get_file_type_from_name(filename=filename, allow_xlsx=self.request.app["ALLOW_XLSX"])

        s3 = self.dl_request.get_s3_service()

        file_exists = await s3_file_exists(s3.client, s3.tmp_bucket_name, s3_key)
        if not file_exists:
            raise exc.DocumentNotFound()

        s3_key_parts = s3_key.split(S3_KEY_PARTS_SEPARATOR)
        if len(s3_key_parts) != 2 or s3_key_parts[0] != self.dl_request.rci.user_id:
            exc.PermissionDenied()

        rmm = self.dl_request.get_redis_model_manager()
        dfile = DataFile(
            s3_key=s3_key,
            manager=rmm,
            filename=filename,
            file_type=file_type,
            status=FileProcessingStatus.in_progress,
        )
        LOGGER.info(f"Data file id: {dfile.id}")

        await dfile.save()

        task_processor = self.dl_request.get_task_processor()
        if file_type == FileType.xlsx:
            await task_processor.schedule(ProcessExcelTask(file_id=dfile.id))
            LOGGER.info(f"Scheduled ProcessExcelTask for file_id {dfile.id}")
        else:
            await task_processor.schedule(ParseFileTask(file_id=dfile.id))
            LOGGER.info(f"Scheduled ParseFileTask for file_id {dfile.id}")

        return web.json_response(
            files_schemas.FileUploadResponseSchema().dump({"file_id": dfile.id, "title": dfile.filename}),
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


class DocumentsView(FileUploaderBaseView):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset()  # Don't skip CSRF check

    FILE_TYPE_TO_DATA_FILE_PREPARER_MAP: dict[
        FileType, Callable[[Optional[str], Optional[str], Optional[str], RedisModelManager], Awaitable[DataFile]]
    ] = {
        FileType.yadocs: yadocs_data_file_preparer,
    }

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(files_schemas.FileDocumentsRequestSchema)

        file_type = FileType.yadocs

        rmm = self.dl_request.get_redis_model_manager()

        oauth_token: Optional[str] = req_data["oauth_token"]
        public_link: Optional[str] = req_data["public_link"]
        private_path: Optional[str] = req_data["private_path"]
        connection_id: Optional[str] = req_data["connection_id"]
        authorized: bool = req_data["authorized"]

        df = await self.FILE_TYPE_TO_DATA_FILE_PREPARER_MAP[file_type](oauth_token, private_path, public_link, rmm)

        LOGGER.info(f"Data file id: {df.id}")
        await df.save()

        task_processor = self.dl_request.get_task_processor()
        await task_processor.schedule(
            DownloadYaDocsTask(
                file_id=df.id,
                authorized=authorized,
                connection_id=connection_id,
            )
        )
        LOGGER.info(f"Scheduled DownloadYaDocsTask for file_id {df.id}")

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

        dfile_by_source_properties: dict[str, DataFile] = {}
        for src in sources:
            source_lock_key, source_lock_token = get_update_connection_source_lock(src["id"])
            LOGGER.info(f"Acquiring redis lock {source_lock_key}")
            # source locks will be released by file uploader worker when the update process is finished
            source_lock = RedisLock(redis, name=source_lock_key, timeout=120, blocking=False)
            if not await source_lock.acquire(token=source_lock_token):
                LOGGER.warning(f"Cannot acquire lock {source_lock_key}, skipping the source")
                continue
            LOGGER.info(f"Lock {source_lock_key} acquired")

            if req_data["type"] == FileType.gsheets:
                if src["spreadsheet_id"] not in dfile_by_source_properties:
                    dfile_by_source_properties[src["spreadsheet_id"]] = DataFile(
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
                LOGGER.info(f'Data file id: {dfile_by_source_properties[src["spreadsheet_id"]].id}')

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

                dfile_by_source_properties[src["spreadsheet_id"]].sources.append(sheet_data_source)  # type: ignore  # 2024-01-30 # TODO: Item "None" of "list[DataSource] | None" has no attribute "append"  [union-attr]

            elif req_data["type"] == FileType.yadocs:
                filled_source_property = "public_link" if src["public_link"] is not None else "private_path"

                if (src["public_link"] is None or src["public_link"] not in dfile_by_source_properties) and (
                    src["private_path"] is None or src["private_path"] not in dfile_by_source_properties
                ):
                    dfile_by_source_properties[src[filled_source_property]] = DataFile(
                        user_id=DataFile.system_user_id if self._is_internal else None,  # filled from rci when None
                        manager=rmm,
                        filename="TITLE",
                        status=FileProcessingStatus.in_progress,
                        file_type=FileType.yadocs,
                        sources=[],
                        user_source_properties=YaDocsUserSourceProperties(
                            private_path=src["private_path"],
                            public_link=src["public_link"],
                            oauth_token=req_data["oauth_token"],
                        ),
                    )
                LOGGER.info(f"Data file id: {dfile_by_source_properties[src[filled_source_property]].id}")

                sheet_data_source = DataSource(
                    id=src["id"],
                    title=src["title"],
                    raw_schema=[],
                    file_source_settings=YaDocsFileSourceSettings(
                        first_line_is_header=src["first_line_is_header"],
                        raw_schema_header=[],
                        raw_schema_body=[],
                    ),
                    user_source_dsrc_properties=YaDocsUserSourceDataSourceProperties(
                        sheet_id=src["sheet_id"],
                    ),
                    status=FileProcessingStatus.in_progress,
                    error=None,
                )

                dfile_by_source_properties[src[filled_source_property]].sources.append(sheet_data_source)  # type: ignore  # 2024-01-30 # TODO: Item "None" of "list[DataSource] | None" has no attribute "append"  [union-attr]

        task_processor = self.dl_request.get_task_processor()
        exec_mode = TaskExecutionMode.UPDATE_AND_SAVE if req_data["save"] else TaskExecutionMode.UPDATE_NO_SAVE
        if req_data.get("tenant_id") is not None:
            tenant_id = req_data["tenant_id"]
        else:
            assert self.dl_request.rci.tenant is not None
            tenant_id = self.dl_request.rci.tenant.get_tenant_id()
        assert tenant_id is not None
        for dfile in dfile_by_source_properties.values():
            await dfile.save()

            if req_data["type"] == FileType.gsheets:
                download_gsheet_task = DownloadGSheetTask(
                    file_id=dfile.id,
                    authorized=req_data["authorized"],
                    tenant_id=tenant_id,
                    connection_id=req_data["connection_id"],
                    exec_mode=exec_mode,
                )
                await task_processor.schedule(download_gsheet_task)
                LOGGER.info(f"Scheduled DownloadGSheetTask for file_id {dfile.id} (update connection)")
            elif req_data["type"] == FileType.yadocs:
                download_yadocs_task = DownloadYaDocsTask(
                    file_id=dfile.id,
                    authorized=req_data["authorized"],
                    tenant_id=tenant_id,
                    connection_id=req_data["connection_id"],
                    exec_mode=exec_mode,
                )
                await task_processor.schedule(download_yadocs_task)
                LOGGER.info(f"Scheduled DownloadYaDocsTask for file_id {dfile.id} (update connection)")

        return web.json_response(
            files_schemas.UpdateConnectionDataResultSchema().dump(dict(files=dfile_by_source_properties.values()))
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

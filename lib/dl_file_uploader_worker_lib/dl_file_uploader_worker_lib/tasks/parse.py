import logging
from typing import (
    Optional,
    Type,
)

import attr

from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.common_locks import release_source_update_locks
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    FileProcessingError,
)
from dl_file_uploader_lib.s3_model.base import S3ModelManager
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
from dl_file_uploader_task_interface.tasks import TaskExecutionMode
from dl_file_uploader_worker_lib.utils.connection_error_tracker import FileConnectionDataSourceErrorTracker
from dl_file_uploader_worker_lib.utils.file_parser import (
    CSVFileParser,
    FileParser,
    SpreadsheetFileParser,
)
from dl_task_processor.task import (
    BaseExecutorTask,
    Fail,
    Retry,
    Success,
    TaskResult,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class ParseFileTask(BaseExecutorTask[task_interface.ParseFileTask, FileUploaderTaskContext]):
    cls_meta = task_interface.ParseFileTask

    FILE_TYPE_TO_PARSER_MAP: dict[FileType, Type[FileParser]] = {
        FileType.csv: CSVFileParser,
        FileType.gsheets: SpreadsheetFileParser,
        FileType.xlsx: SpreadsheetFileParser,
        FileType.yadocs: SpreadsheetFileParser,
    }

    async def run(self) -> TaskResult:
        dfile: Optional[DataFile] = None
        usm = self._ctx.get_async_usm()
        usm.set_tenant_override(self._ctx.tenant_resolver.resolve_tenant_def_by_tenant_id(self.meta.tenant_id))
        task_processor = self._ctx.make_task_processor(self._request_id)
        redis = self._ctx.redis_service.get_redis()
        connection_error_tracker = FileConnectionDataSourceErrorTracker(
            usm=usm,
            task_processor=task_processor,
            redis=redis,
            tenant_id=self.meta.tenant_id,
            request_id=self._request_id,
        )
        try:
            LOGGER.info(f"ParseFileTask. Mode: {self.meta.exec_mode.name}. File: {self.meta.file_id}")

            redis = self._ctx.redis_service.get_redis()
            rmm = RedisModelManager(redis=redis, crypto_keys_config=self._ctx.crypto_keys_config)

            # TODO(catsona): Remove after release
            s3mm = None
            if self.meta.tenant_id is not None:
                s3mm = S3ModelManager(
                    s3_service=self._ctx.s3_service,
                    crypto_keys_config=self._ctx.crypto_keys_config,
                    tenant_id=self.meta.tenant_id,
                )

            dfile = await DataFile.get(manager=rmm, obj_id=self.meta.file_id)
            assert dfile is not None

            assert dfile.file_type is not None
            file_parser_cls = self.FILE_TYPE_TO_PARSER_MAP[dfile.file_type]

            file_parser = file_parser_cls(
                dfile=dfile,
                s3=self._ctx.s3_service,
                tpe=self._ctx.tpe,
                file_settings=self.meta.file_settings,
                source_id=self.meta.source_id,
            )

            for dsrc in file_parser.ensure_sources_and_return():
                if not dsrc.is_applicable:
                    if dsrc.error is None:  # all existing errors should've been saved by now
                        dsrc.error = FileProcessingError.from_exception(exc.InvalidSource())
                        connection_error_tracker.add_error(dsrc.id, dsrc.error)
                    continue

                try:
                    has_header, raw_schema, file_settings, dsrc_settings = await file_parser.guess_header_and_schema(
                        dsrc
                    )
                except exc.DLFileUploaderBaseError as e:
                    dsrc.status = FileProcessingStatus.failed
                    dsrc.error = FileProcessingError.from_exception(e)
                    connection_error_tracker.add_error(dsrc.id, dsrc.error)
                    continue

                dfile.file_settings = file_settings
                dsrc.raw_schema = raw_schema
                dsrc.file_source_settings = dsrc_settings

                # TODO(catsona): Remove after release, fallback to old behavior
                if self.meta.tenant_id is None:
                    redis_preview = await file_parser.prepare_preview_legacy(dsrc, rmm)
                    await redis_preview.save(
                        ttl=12 * 12 * 60
                    )  # save preview temporarily, so it is deleted if source never gets saved
                    dsrc.preview_id = redis_preview.id
                    LOGGER.debug(f"Saving preview {redis_preview.id} to redis")

                else:
                    assert s3mm is not None
                    preview = await file_parser.prepare_preview(dsrc, s3mm)
                    await preview.save(
                        persistent=False,
                    )  # save preview temporarily, so it is deleted if source never gets saved
                    dsrc.preview_id = preview.id
                    LOGGER.debug(f"Saving preview {preview.id} to s3")

                LOGGER.info("DataSourcePreview object saved.")

                dsrc.status = FileProcessingStatus.ready

            dfile.status = FileProcessingStatus.ready
            if dfile.file_type == FileType.csv and (csv_src := dfile.sources[0]).status == FileProcessingStatus.failed:  # type: ignore  # 2024-01-30 # TODO: Value of type "list[DataSource] | None" is not indexable  [index]
                # there is guaranteed a single source in a csv file, and the error must be saved on the top level
                dfile.status = csv_src.status
                dfile.error = csv_src.error

            await connection_error_tracker.finalize(self.meta.exec_mode, self.meta.connection_id)
            await dfile.save()
            LOGGER.info("DataFile object saved.")

            if self.meta.exec_mode == TaskExecutionMode.UPDATE_AND_SAVE:
                for dsrc in dfile.sources or ():
                    if not dsrc.is_applicable:
                        continue

                    assert self.meta.tenant_id is not None and self.meta.connection_id is not None
                    save_source_task = task_interface.SaveSourceTask(
                        tenant_id=self.meta.tenant_id,
                        file_id=dfile.id,
                        src_source_id=dsrc.id,
                        dst_source_id=dsrc.id,
                        connection_id=self.meta.connection_id,
                        exec_mode=self.meta.exec_mode,
                    )
                    await task_processor.schedule(save_source_task)
                    LOGGER.info(f"Scheduled SaveSourceTask for file_id {dfile.id}")
            elif self.meta.exec_mode == TaskExecutionMode.UPDATE_NO_SAVE and dfile.sources:
                await release_source_update_locks(redis, *[dsrc.id for dsrc in dfile.sources])

        except Exception as ex:
            LOGGER.exception(ex)
            if dfile is None:
                return Retry(attempts=3)
            else:
                dfile.status = FileProcessingStatus.failed
                exc_to_save = ex if isinstance(ex, exc.DLFileUploaderBaseError) else exc.ParseFailed()
                dfile.error = FileProcessingError.from_exception(exc_to_save)
                await dfile.save()

                for src in dfile.sources or ():
                    connection_error_tracker.add_error(src.id, dfile.error)
                await connection_error_tracker.finalize(self.meta.exec_mode, self.meta.connection_id)
                return Fail()
        finally:
            await usm.close()
        return Success()

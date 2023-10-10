import pytest

from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    YaDocumentsUserSourceProperties,
)
from dl_file_uploader_task_interface.tasks import DownloadYaDocumentsTask
from dl_task_processor.state import wait_task
from dl_testing.s3_utils import s3_file_exists


@pytest.mark.asyncio
async def test_download_yadocs_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
):
    df = DataFile(
        filename="",
        file_type=FileType.yadocuments,
        manager=redis_model_manager,
        status=FileProcessingStatus.in_progress,
        user_source_properties=YaDocumentsUserSourceProperties(public_link="https://disk.yandex.lt/i/OyzdmFI0MUEEgA"),
    )
    await df.save()

    task = await task_processor_client.schedule(DownloadYaDocumentsTask(file_id=df.id))
    result = await wait_task(task, task_state)

    assert result[-1] == "success"
    assert await s3_file_exists(key=df.s3_key, bucket=s3_tmp_bucket, s3_client=s3_client)

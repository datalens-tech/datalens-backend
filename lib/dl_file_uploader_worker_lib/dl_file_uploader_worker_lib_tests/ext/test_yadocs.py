import pytest

from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    YaDocsUserSourceProperties,
)
from dl_file_uploader_task_interface.tasks import DownloadYaDocsTask
from dl_task_processor.state import wait_task
from dl_testing.s3_utils import s3_file_exists


@pytest.mark.asyncio
async def test_download_yadocs_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    reader_app,
):
    dfile = DataFile(
        filename="",
        file_type=FileType.yadocs,
        manager=redis_model_manager,
        status=FileProcessingStatus.in_progress,
        user_source_properties=YaDocsUserSourceProperties(public_link="https://disk.yandex.lt/i/OyzdmFI0MUEEgA"),
    )
    await dfile.save()

    task = await task_processor_client.schedule(DownloadYaDocsTask(file_id=dfile.id, authorized=False))
    result = await wait_task(task, task_state)

    assert result[-1] == "success"
    assert await s3_file_exists(key=dfile.s3_key_old, bucket=s3_tmp_bucket, s3_client=s3_client)

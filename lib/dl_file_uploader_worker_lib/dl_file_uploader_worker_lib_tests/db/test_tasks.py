from datetime import (
    datetime,
    timedelta,
)
import logging
from typing import Any
import uuid

from botocore.exceptions import ClientError
import pytest
import pytest_asyncio

from dl_configs.crypto_keys import get_dummy_crypto_keys_config
from dl_constants.enums import (
    FileProcessingStatus,
    UserDataType,
)
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.enums import (
    CSVEncoding,
    FileType,
    RenameTenantStatus,
)
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    RenameTenantStatusModel,
)
from dl_file_uploader_lib.s3_model.base import (
    S3ModelManager,
    S3ModelNotFound,
)
from dl_file_uploader_lib.s3_model.models import S3DataSourcePreview
from dl_file_uploader_lib.testing.data_gen import generate_sample_csv_data_str
from dl_file_uploader_task_interface.tasks import (
    CleanS3LifecycleRulesTask,
    CleanupTenantFilePreviewsTask,
    CleanupTenantTask,
    DeleteFileTask,
    ParseFileTask,
    RenameTenantFilesTask,
    SaveSourceTask,
)
from dl_file_uploader_worker_lib.utils import parsing_utils
from dl_task_processor.state import wait_task

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection

from .utils import create_file_connection


LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_parse_file_task(
    task_processor_client,
    task_state,
    s3_client,
    s3_model_manager,
    redis_model_manager,
    uploaded_file_id,
):
    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=uploaded_file_id)
    assert df.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=uploaded_file_id,
            tenant_id="common",
        )
    )
    await wait_task(task, task_state)

    df = await DataFile.get(manager=rmm, obj_id=uploaded_file_id)
    assert df.id == uploaded_file_id
    assert df.status == FileProcessingStatus.ready

    assert len(df.sources) == 1
    dsrc = df.sources[0]
    assert dsrc.status == FileProcessingStatus.ready
    assert dsrc.title == "test_file.csv"
    assert [sch.user_type for sch in dsrc.raw_schema] == [
        UserDataType.string,
        UserDataType.integer,
        UserDataType.float,
        UserDataType.date,
        UserDataType.genericdatetime,
    ]
    assert [sch.name for sch in dsrc.raw_schema] == ["f1", "f2", "f3", "data", "data_i_vremya"]
    assert [sch.title for sch in dsrc.raw_schema] == ["f1", "f2", "f3", "Дата", "Дата и время"]

    preview = await S3DataSourcePreview.get(manager=s3_model_manager, obj_id=dsrc.preview_id)
    assert preview.id == dsrc.preview_id
    assert preview.preview_data == [
        ["qwe", "123", "45.9", "2021-02-04", "2021-02-04 12:00:00"],
        ["asd", "345", "47.9", "2021-02-05", "2021-02-05 14:01:00"],
        ["zxc", "456", "49,9", "2021-02-06", "2021-02-06 11:59:00"],
        ["zxc", "456", None, None, "2021-02-06 11:59:00"],
        ["zxczxczxcz...czxczxczxc", "456", "49.9", "2021-02-06", None],
    ]


@pytest.mark.asyncio
async def test_parse_file_task_with_file_settings(
    task_processor_client,
    task_state,
    s3_client,
    s3_model_manager,
    redis_model_manager,
    uploaded_file_id,
):
    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=uploaded_file_id)
    assert df.status == FileProcessingStatus.in_progress
    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=uploaded_file_id,
            tenant_id="common",
        )
    )
    await wait_task(task, task_state)

    df = await DataFile.get(manager=rmm, obj_id=uploaded_file_id)
    assert df.status == FileProcessingStatus.ready
    dsrc = df.sources[0]
    assert dsrc.status == FileProcessingStatus.ready

    source_id = dsrc.id
    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=uploaded_file_id,
            source_id=source_id,
            file_settings=dict(encoding="utf8", delimiter="tab", first_line_is_header=False),
            source_settings={},
            tenant_id="common",
        )
    )
    await wait_task(task, task_state)

    df = await DataFile.get(manager=rmm, obj_id=uploaded_file_id)
    assert df.status == FileProcessingStatus.ready
    dsrc = df.sources[0]
    assert dsrc.id == source_id
    assert dsrc.status == FileProcessingStatus.ready
    assert len(dsrc.raw_schema) == 1
    assert dsrc.file_source_settings.first_line_is_header is False
    assert df.file_settings.dialect.delimiter == "\t"
    assert df.file_settings.encoding == CSVEncoding.utf8

    preview = await S3DataSourcePreview.get(manager=s3_model_manager, obj_id=dsrc.preview_id)
    assert preview.id == dsrc.preview_id
    assert len(preview.preview_data) == 6


@pytest.mark.asyncio
async def test_parse_10mb_file_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    uploaded_10mb_file_id,
):
    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=uploaded_10mb_file_id)
    assert df.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=uploaded_10mb_file_id,
            tenant_id="common",
        )
    )
    await wait_task(task, task_state)

    df = await DataFile.get(manager=rmm, obj_id=uploaded_10mb_file_id)
    assert df.status == FileProcessingStatus.ready


@pytest.mark.asyncio
async def test_save_source_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    uploaded_file_id,
    default_async_usm_per_test,
    read_chs3_file,
):
    usm = default_async_usm_per_test
    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=uploaded_file_id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    df = await DataFile.get(manager=redis_model_manager, obj_id=uploaded_file_id)
    source = df.sources[0]
    assert source.status == FileProcessingStatus.ready

    conn = await create_file_connection(usm, df.id, source.id, source.raw_schema)
    assert conn.get_file_source_by_id(source.id).status == FileProcessingStatus.in_progress

    task_save = await task_processor_client.schedule(
        SaveSourceTask(
            tenant_id="common",
            file_id=uploaded_file_id,
            src_source_id=source.id,
            dst_source_id=source.id,
            connection_id=conn.uuid,
        )
    )
    await wait_task(task_save, task_state)

    conn1 = await usm.get_by_id(conn.uuid)
    assert conn1.get_file_source_by_id(source.id).status == FileProcessingStatus.ready


@pytest.mark.asyncio
async def test_save_source_task_dt(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    uploaded_file_dt_id,
    default_async_usm_per_test,
    read_chs3_file,
):
    usm = default_async_usm_per_test
    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=uploaded_file_dt_id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    df = await DataFile.get(manager=redis_model_manager, obj_id=uploaded_file_dt_id)
    source = df.sources[0]
    assert source.status == FileProcessingStatus.ready

    conn = await create_file_connection(usm, df.id, source.id, source.raw_schema)
    assert conn.get_file_source_by_id(source.id).status == FileProcessingStatus.in_progress

    task_save = await task_processor_client.schedule(
        SaveSourceTask(
            tenant_id="common",
            file_id=uploaded_file_dt_id,
            src_source_id=source.id,
            dst_source_id=source.id,
            connection_id=conn.uuid,
        )
    )
    await wait_task(task_save, task_state)

    conn1 = await usm.get_by_id(conn.uuid)
    file = conn1.get_file_source_by_id(source.id)
    assert file.status == FileProcessingStatus.ready

    res = read_chs3_file(file.s3_filename)

    assert [str(r) for r, in res] == [
        "2023-10-13 21:02:36",
        "2023-10-13 10:38:48",
        "2023-10-12 20:47:22",
        "2023-10-12 20:46:24",
        "2023-10-12 20:44:19",
    ]


@pytest.mark.asyncio
async def test_save_source_task_on_replace(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    uploaded_file_id,
    another_uploaded_file_id,
    default_async_usm_per_test,
):
    usm = default_async_usm_per_test
    df = []
    source = []

    for file_id in (uploaded_file_id, another_uploaded_file_id):
        task = await task_processor_client.schedule(
            ParseFileTask(
                file_id=file_id,
                tenant_id="common",
            )
        )
        await wait_task(task, task_state)
        new_df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
        df.append(new_df)
        new_source = new_df.sources[0]
        source.append(new_source)
        assert new_source.status == FileProcessingStatus.ready

    conn = await create_file_connection(usm, df[0].id, source[1].id, source[0].raw_schema)
    assert conn.get_file_source_by_id(source[1].id).status == FileProcessingStatus.in_progress

    task_save = await task_processor_client.schedule(
        SaveSourceTask(
            tenant_id="common",
            file_id=uploaded_file_id,
            src_source_id=source[0].id,
            dst_source_id=source[1].id,
            connection_id=conn.uuid,
        )
    )
    await wait_task(task_save, task_state)

    conn = await usm.get_by_id(conn.uuid)
    assert conn.get_file_source_by_id(source[1].id).status == FileProcessingStatus.ready


@pytest.mark.asyncio
async def test_delete_file_task(
    task_processor_client,
    task_state,
    s3_client,
    s3_persistent_bucket,
    redis_model_manager,
    uploaded_file,
):
    filename = uploaded_file.filename

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=uploaded_file.id,
            tenant_id="common",
        )
    )
    await wait_task(task, task_state)

    df = await DataFile.get(manager=redis_model_manager, obj_id=uploaded_file.id)
    source = df.sources[0]

    task = await task_processor_client.schedule(
        DeleteFileTask(
            s3_filename=filename,
            tenant_id="common",
            preview_id=source.preview_id,
        )
    )
    await wait_task(task, task_state)

    with pytest.raises(ClientError) as ex:
        await s3_client.head_object(
            Bucket=s3_persistent_bucket,
            Key=filename,
        )
        assert ex["ResponseMetadata"]["HTTPStatusCode"] == 404

    # and now try with not existing file
    task = await task_processor_client.schedule(
        DeleteFileTask(
            s3_filename=filename,
            tenant_id="common",
            preview_id=source.preview_id,
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"


@pytest_asyncio.fixture(scope="function")
async def files_with_tenant_prefixes(s3_persistent_bucket, s3_client) -> list[str]:
    csv_data = """f1,f2,f3,Дата,Дата и время
qwe,123,45.9,2021-02-04,2021-02-04 12:00:00
asd,345,47.9,2021-02-05,2021-02-05 14:01:00""".encode(
        "utf-8"
    )

    tenant_ids = [f"tenant_{uuid.uuid4()}" for _ in range(3)]
    for tenant_id in tenant_ids:
        for idx in range(42):
            filename = f"file_{idx}"
            await s3_client.put_object(
                ACL="private",
                Bucket=s3_persistent_bucket,
                Key=f"{tenant_id}_{filename}",
                Body=csv_data,
            )

    return tenant_ids


async def get_lc_rules_number(s3_client, bucket) -> int:
    try:
        lc_config = await s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            lc_config = {"Rules": []}
        else:
            raise
    return len(lc_config["Rules"])


@pytest.mark.asyncio
async def test_cleanup_tenant_task(
    task_processor_client,
    task_state,
    s3_client,
    s3_persistent_bucket,
    files_with_tenant_prefixes,
):
    await s3_client.delete_bucket_lifecycle(Bucket=s3_persistent_bucket)
    tenant_ids = files_with_tenant_prefixes

    n_lc_rules = await get_lc_rules_number(s3_client, s3_persistent_bucket)
    assert n_lc_rules == 0

    for tenant_id in tenant_ids:
        task = await task_processor_client.schedule(
            CleanupTenantTask(
                tenant_id=tenant_id,
            )
        )
        result = await wait_task(task, task_state)
        assert result[-1] == "success"

        new_n_lc_rules = await get_lc_rules_number(s3_client, s3_persistent_bucket)
        assert new_n_lc_rules == n_lc_rules + 1
        n_lc_rules = new_n_lc_rules

    assert n_lc_rules == len(tenant_ids)


def make_cleanup_rule(prefix: str) -> dict[str, Any]:
    return {
        "ID": prefix,
        "Expiration": {
            "Date": datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2),
        },
        "Filter": {
            "Prefix": prefix,
        },
        "Status": "Enabled",
    }


@pytest.mark.asyncio
async def test_cleanup_tenant_task_no_files(
    task_processor_client,
    task_state,
    s3_client,
    s3_persistent_bucket,
):
    await s3_client.delete_bucket_lifecycle(Bucket=s3_persistent_bucket)

    task = await task_processor_client.schedule(
        CleanupTenantTask(
            tenant_id="there are no files in this tenant",
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    assert await get_lc_rules_number(s3_client, s3_persistent_bucket) == 1  # note: the LC rule is still created


@pytest.mark.asyncio
async def test_regular_bucket_lifecycle_cleanup_task(
    task_processor_client,
    task_state,
    s3_client,
    s3_persistent_bucket,
    files_with_tenant_prefixes,
):
    tenant_ids = files_with_tenant_prefixes
    await s3_client.delete_bucket_lifecycle(Bucket=s3_persistent_bucket)

    async def put_lifecycle_config(lc_rules: list[dict[str, Any]]) -> None:
        await s3_client.put_bucket_lifecycle_configuration(
            Bucket=s3_persistent_bucket,
            LifecycleConfiguration=dict(
                Rules=lc_rules,
            ),
        )

    rules = [make_cleanup_rule(str(uuid.uuid4())) for _ in range(100)]
    await put_lifecycle_config(rules)

    task = await task_processor_client.schedule(CleanS3LifecycleRulesTask())
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
    new_n_lc_rules = await get_lc_rules_number(s3_client, s3_persistent_bucket)
    assert new_n_lc_rules == 0

    rules = [make_cleanup_rule(str(uuid.uuid4())) for _ in range(100)] + [make_cleanup_rule(tenant_ids[0])]
    await put_lifecycle_config(rules)

    task = await task_processor_client.schedule(CleanS3LifecycleRulesTask())
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
    new_n_lc_rules = await get_lc_rules_number(s3_client, s3_persistent_bucket)
    assert new_n_lc_rules == 1


@pytest.mark.asyncio
async def test_datetime64(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    default_async_usm_per_test,
):
    usm = default_async_usm_per_test
    csv_data = """utc_start_dttm
2022-06-01 00:36:55.845368
2022-06-01 00:39:00.91446
2022-06-01 00:52:03.14199
2022-06-01 10:48:45.272827""".encode(
        "utf-8"
    )

    rmm = redis_model_manager
    dfile = DataFile(
        manager=rmm,
        filename="test_file.csv",
        file_type=FileType.csv,
        size=len(csv_data),
        status=FileProcessingStatus.in_progress,
    )
    await s3_client.put_object(
        ACL="private",
        Bucket=s3_tmp_bucket,
        Key=dfile.s3_key_old,
        Body=csv_data,
    )
    await dfile.save()

    dfile = await DataFile.get(manager=rmm, obj_id=dfile.id)
    assert dfile.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=dfile.id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    dfile = await DataFile.get(manager=rmm, obj_id=dfile.id)
    assert dfile.status == FileProcessingStatus.ready

    assert len(dfile.sources) == 1
    source = dfile.sources[0]
    assert source.status == FileProcessingStatus.ready

    conn = await create_file_connection(usm, dfile.id, source.id, source.raw_schema)
    assert conn.get_file_source_by_id(source.id).status == FileProcessingStatus.in_progress

    task_save = await task_processor_client.schedule(
        SaveSourceTask(
            file_id=dfile.id,
            tenant_id="common",
            src_source_id=source.id,
            dst_source_id=source.id,
            connection_id=conn.uuid,
        )
    )
    result = await wait_task(task_save, task_state)
    assert result[-1] == "success"

    conn = await usm.get_by_id(conn.uuid)
    assert conn.get_file_source_by_id(source.id).status == FileProcessingStatus.ready


@pytest.mark.asyncio
async def test_datetime_tz(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    default_async_usm_per_test,
):
    csv_data = """dt
2022-07-01 13:52:07+03
2022-07-01 14:34:36+03
2022-07-01 14:38:47+03
2022-07-02 12:33:20+03""".encode(
        "utf-8"
    )

    usm = default_async_usm_per_test
    rmm = redis_model_manager
    dfile = DataFile(
        manager=rmm,
        filename="test_file.csv",
        file_type=FileType.csv,
        size=len(csv_data),
        status=FileProcessingStatus.in_progress,
    )
    await s3_client.put_object(
        ACL="private",
        Bucket=s3_tmp_bucket,
        Key=dfile.s3_key_old,
        Body=csv_data,
    )
    await dfile.save()

    dfile = await DataFile.get(manager=rmm, obj_id=dfile.id)
    assert dfile.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=dfile.id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    dfile = await DataFile.get(manager=rmm, obj_id=dfile.id)
    assert dfile.status == FileProcessingStatus.ready

    assert len(dfile.sources) == 1
    source = dfile.sources[0]
    assert source.status == FileProcessingStatus.ready

    conn = await create_file_connection(usm, dfile.id, source.id, source.raw_schema)
    assert conn.get_file_source_by_id(source.id).status == FileProcessingStatus.in_progress

    task_save = await task_processor_client.schedule(
        SaveSourceTask(
            file_id=dfile.id,
            tenant_id="common",
            src_source_id=source.id,
            dst_source_id=source.id,
            connection_id=conn.uuid,
        )
    )
    result = await wait_task(task_save, task_state)
    assert result[-1] == "success"

    conn = await usm.get_by_id(conn.uuid)
    assert conn.get_file_source_by_id(source.id).status == FileProcessingStatus.ready


@pytest.mark.asyncio
async def test_cleanup_tenant_file_previews_task(
    redis_model_manager,
    task_processor_client,
    task_state,
    s3_client,
    s3_service,
    s3_persistent_bucket,
):
    tenant_id = str(uuid.uuid4())[:20]

    s3_model_manager = S3ModelManager(
        s3_service=s3_service,
        tenant_id=tenant_id,
        crypto_keys_config=get_dummy_crypto_keys_config(),
    )

    n_previews = 5
    preview_ids = []

    # Generate tmp previews
    for _ in range(n_previews):
        preview = S3DataSourcePreview(manager=s3_model_manager, preview_data=[])
        await preview.save(persistent=False)

        preview_ids.append(preview.id)

    # Ensure existing
    for preview_id in preview_ids:
        preview = await S3DataSourcePreview.get(manager=s3_model_manager, obj_id=preview_id)
        assert preview
        assert preview.id == preview_id

    # Generate persistent previews
    for _ in range(n_previews):
        preview = S3DataSourcePreview(manager=s3_model_manager, preview_data=[])
        await preview.save(persistent=True)

        preview_ids.append(preview.id)

    # Delete previews action
    task = await task_processor_client.schedule(
        CleanupTenantFilePreviewsTask(
            tenant_id=tenant_id,
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    for preview_id in preview_ids:
        with pytest.raises(S3ModelNotFound):
            await S3DataSourcePreview.get(manager=s3_model_manager, obj_id=preview_id)


@pytest.mark.asyncio
async def test_too_many_columns_csv(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    monkeypatch,
):
    monkeypatch.setattr(parsing_utils, "MAX_COLUMNS_COUNT", 10)
    csv_data = generate_sample_csv_data_str(3, 11).encode("utf-8")

    rmm = redis_model_manager
    dfile = DataFile(
        manager=rmm,
        filename="too_many_columns.csv",
        file_type=FileType.csv,
        size=len(csv_data),
        status=FileProcessingStatus.in_progress,
    )
    await s3_client.put_object(
        ACL="private",
        Bucket=s3_tmp_bucket,
        Key=dfile.s3_key_old,
        Body=csv_data,
    )
    await dfile.save()

    dfile = await DataFile.get(manager=rmm, obj_id=dfile.id)
    assert dfile.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=dfile.id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    dfile = await DataFile.get(manager=rmm, obj_id=dfile.id)
    assert dfile.status == FileProcessingStatus.failed
    assert dfile.error is not None and dfile.error.code == exc.TooManyColumnsError.err_code

    assert len(dfile.sources) == 1
    assert dfile.sources[0].error.code == exc.TooManyColumnsError.err_code


@pytest.mark.asyncio
async def test_rename_tenant_files(
    saved_file_connection_id,
    task_processor_client,
    task_state,
    s3_client,
    s3_persistent_bucket,
    default_async_usm_per_test,
    redis_model_manager,
):
    usm = default_async_usm_per_test
    conn = await usm.get_by_id(saved_file_connection_id)
    source = conn.data.sources[0]
    assert source.s3_filename_suffix
    s3_obj = await s3_client.get_object(
        Bucket=s3_persistent_bucket, Key=conn.get_full_s3_filename(source.s3_filename_suffix)
    )
    s3_data = await s3_obj["Body"].read()

    new_tenant_id = str(uuid.uuid4())
    task = await task_processor_client.schedule(
        RenameTenantFilesTask(
            tenant_id=new_tenant_id,
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    updated_conn = await usm.get_by_id(saved_file_connection_id)
    assert isinstance(conn, BaseFileS3Connection)
    updated_source = updated_conn.get_file_source_by_id(source.id)
    assert updated_source.s3_filename_suffix
    assert updated_source.s3_filename and updated_source.s3_filename.startswith(updated_conn.raw_tenant_id)
    new_s3_filename = updated_conn.get_full_s3_filename(updated_source.s3_filename_suffix)
    updated_s3_obj = await s3_client.get_object(Bucket=s3_persistent_bucket, Key=new_s3_filename)
    updated_s3_obj_data = await updated_s3_obj["Body"].read()
    assert s3_data == updated_s3_obj_data

    status_obj = await RenameTenantStatusModel.get(manager=redis_model_manager, obj_id=new_tenant_id)
    assert status_obj.status == RenameTenantStatus.success

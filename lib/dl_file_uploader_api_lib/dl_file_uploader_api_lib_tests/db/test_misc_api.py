import asyncio
import json
import time
import uuid

import pytest

from dl_constants.api_constants import DLHeadersCommon
from dl_file_uploader_api_lib_tests.req_builder import ReqBuilder
from dl_testing.s3_utils import get_lc_rules_number


@pytest.fixture(scope="function")
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


@pytest.mark.asyncio
async def test_cleanup_tenant(
    master_token_header,
    fu_client,
    s3_client,
    s3_persistent_bucket: str,
    files_with_tenant_prefixes: list[str],
):
    await s3_client.delete_bucket_lifecycle(Bucket=s3_persistent_bucket)
    tenant_ids = files_with_tenant_prefixes

    n_lc_rules = await get_lc_rules_number(s3_client, s3_persistent_bucket)
    assert n_lc_rules == 0

    for tenant_id in tenant_ids:
        resp = await fu_client.make_request(ReqBuilder.cleanup(tenant_id, master_token_header))
        await asyncio.sleep(3)
        assert resp.status == 200, resp.json

        new_n_lc_rules = await get_lc_rules_number(s3_client, s3_persistent_bucket)
        assert new_n_lc_rules == n_lc_rules + 1
        n_lc_rules = new_n_lc_rules

    assert n_lc_rules == len(tenant_ids)


@pytest.mark.asyncio
async def test_cleanup_tenant_no_files(
    s3_client,
    master_token_header,
    fu_client,
    s3_persistent_bucket: str,
):
    await s3_client.delete_bucket_lifecycle(Bucket=s3_persistent_bucket)

    resp = await fu_client.make_request(ReqBuilder.cleanup("there are no files in this tenant", master_token_header))
    assert resp.status == 200, resp.json
    await asyncio.sleep(3)

    assert await get_lc_rules_number(s3_client, s3_persistent_bucket) == 1  # note: the LC rule is still created


@pytest.mark.asyncio
async def test_cleanup_tenant_invalid_master_token(fu_client):
    resp = await fu_client.make_request(
        ReqBuilder.cleanup(
            tenant_id="does not matter",
            master_token_header={DLHeadersCommon.FILE_UPLOADER_MASTER_TOKEN: "invalid-master-token"},
            require_ok=False,
        )
    )
    assert resp.status == 403


@pytest.mark.asyncio
async def test_cleanup_tenant_no_master_token(fu_client):
    resp = await fu_client.make_request(
        ReqBuilder.cleanup(
            tenant_id="does not matter",
            master_token_header=None,
            require_ok=False,
        )
    )
    assert resp.status == 403


@pytest.mark.asyncio
async def test_rename_tenant_files(master_token_header, fu_client, redis_cli):
    tenant_id = str(uuid.uuid4())
    start_time = time.time()
    resp = await fu_client.make_request(ReqBuilder.rename_tenant_files(master_token_header, tenant_id))
    assert resp.status == 200, resp.json

    await asyncio.sleep(3)
    redis_raw_data = await redis_cli.get(f"rename_tenant_status/{tenant_id}")
    redis_data = json.loads(redis_raw_data)
    assert redis_data["id"] == tenant_id
    assert redis_data["status"] == "success"
    assert start_time < redis_data["mtime"] < time.time()

from __future__ import annotations

import logging

import pytest

from bi_legacy_test_bundle_tests.core.fixtures_ce import (  # noqa: F401
    query_executor_app,
    query_executor_options,
    sync_remote_query_executor,
)

LOGGER = logging.getLogger(__name__)


CSV_DATA = b"""asdf,12
qwer,13
zxcv,14
"""


@pytest.fixture(scope="function")
async def s3_csv(loop, s3_settings, s3_client, s3_bucket):
    filename = "test_data.csv"
    data = CSV_DATA
    await s3_client.put_object(
        Bucket=s3_bucket,
        Key=filename,
        Body=data,
    )

    yield filename

    await s3_client.delete_object(Bucket=s3_bucket, Key=filename)

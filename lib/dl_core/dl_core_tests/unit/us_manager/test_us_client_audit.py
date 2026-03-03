import pytest
import responses

from dl_constants.api_constants import DLHeadersCommon
from dl_core.enums import USApiType
from dl_core.united_storage_client import (
    USAuthContextMaster,
    UStorageClient,
)
import dl_retrier


@pytest.fixture()
def us_client() -> UStorageClient:
    return UStorageClient(
        host="http://us-host",
        prefix=USApiType.v1.value,
        auth_ctx=USAuthContextMaster(us_master_token="fake_token"),
        retry_policy_factory=dl_retrier.DefaultRetryPolicyFactory(),
    )


@pytest.mark.parametrize(
    "audit_mode,expected_url",
    [
        (False, "http://us-host/v1/entries/some-id"),
        (True, "http://us-host/audit/v1/entries/some-id"),
    ],
    ids=["default_prefix", "audit_prefix"],
)
def test_get_full_url_audit_mode(us_client: UStorageClient, audit_mode: bool, expected_url: str):
    url = us_client._get_full_url("/entries/some-id", audit_mode=audit_mode)
    assert url == expected_url


@responses.activate
@pytest.mark.parametrize(
    "context_headers,expected_prefix",
    [
        ({DLHeadersCommon.AUDIT_MODE.value: "true"}, "audit/v1"),
        ({DLHeadersCommon.DATASET_ID.value: "ds-123"}, "v1"),
        ({}, "v1"),
    ],
    ids=["audit_header_present", "other_header_only", "empty_context"],
)
def test_request_uses_correct_url_prefix(
    us_client: UStorageClient,
    context_headers: dict,
    expected_prefix: str,
):
    responses.add(responses.GET, f"http://us-host/{expected_prefix}/entries/some-id", json={}, status=200)

    us_client.set_context("connection", context_headers)
    us_client._request(
        UStorageClient.RequestData(
            method="get",
            relative_url="/entries/some-id",
            params=None,
            json=None,
        ),
        context_name="connection",
    )

    assert len(responses.calls) == 1
    assert f"/{expected_prefix}/entries/some-id" in responses.calls[0].request.url

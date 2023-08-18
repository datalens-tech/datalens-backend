from typing import Optional, Sequence

from bi_cloud_integration.exc import DefaultYCExcInfo, YCPermissionDenied
from bi_cloud_integration.model import IAMResource
from bi_cloud_integration.yc_as_client import DLASClient


async def authorize_mock(
    self: DLASClient,
    permission: str, resource_path: Sequence[IAMResource],
    iam_token: str,
    request_id: Optional[str] = None,
) -> None:
    expected_resource_path = [IAMResource.folder('folder_1')]
    try:
        assert permission == 'datalens.instances.admin', f'Invalid permission: {permission} != datalens.instances.admin'
        assert resource_path == expected_resource_path, f'Invalid resource_path: {resource_path} != ' \
                                                        f'{expected_resource_path}'
        assert iam_token == 'dummy_iam_token', f'Invalid IAM token: {iam_token} != dummy_iam_token'
    except AssertionError as e:
        raise YCPermissionDenied(DefaultYCExcInfo(internal_details=str(e)))

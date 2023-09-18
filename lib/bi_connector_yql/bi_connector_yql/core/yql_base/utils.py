from __future__ import annotations

from typing import TYPE_CHECKING

from bi_cloud_integration.exc import YCPermissionDenied
from bi_cloud_integration.model import IAMResource

from dl_core import exc
from dl_api_commons.base_models import RequestContextInfo
from bi_api_commons_ya_cloud.models import IAMAuthData
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry

if TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry


async def validate_service_account_id(
        service_account_id: str,
        context: RequestContextInfo,
        services_registry: ServicesRegistry,
) -> None:
    auth_data = context.auth_data
    if not isinstance(auth_data, IAMAuthData) or not auth_data.iam_token:
        # TODO: actually go and make an iam_token from any other authentication (cookie / oauth).
        # Relevant for the yateam installation.
        raise exc.PlatformPermissionRequired('Need to be iam-authenticated to use a service account')
    iam_token = auth_data.iam_token

    yc_sr = services_registry.get_installation_specific_service_registry(YCServiceRegistry)
    as_cli = await yc_sr.get_yc_as_client()
    if not as_cli:
        raise Exception("YC Access Service client is not available")

    assert context.tenant is not None
    try:
        with as_cli:
            await as_cli.authorize(
                iam_token=iam_token,
                permission='iam.serviceAccounts.use',
                resource_path=[IAMResource.service_account(service_account_id)]
            )
    except YCPermissionDenied:
        raise exc.PlatformPermissionRequired('Not allowed to use the specified service account')

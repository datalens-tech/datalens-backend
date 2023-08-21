import functools
import logging
import uuid
from typing import Optional, Callable

import aiohttp

from bi_api_commons.base_models import IAMAuthData, TenantDef
from bi_api_commons.client.base import get_default_aiohttp_session
from bi_testing_ya.cloud_tokens import AccountCredentials
from bi_us_client.us_workbook_cmd_client import USWorkbookCommandClient

NOT_REQUIRED = 'NOT_REQUIRED'
CREATE_ONLY = 'CREATE_ONLY'
CREATE_AND_DELETE_AFTERWARDS = 'CREATE_AND_DELETE_AFTERWARDS'


def workbook_mgmt_hooks(
        workbook_mgmt_strategy: str,
        us_lb_main_base_url: str,
        dynamic_env_service_accounts: dict[str, AccountCredentials],
        tenant: TenantDef
):
    if workbook_mgmt_strategy == NOT_REQUIRED:
        return _blank_create_workbook_id_hook, _blank_delete_workbook_id_hook
    req_id = uuid.uuid4().hex
    client_factory: Callable[[aiohttp.ClientSession], USWorkbookCommandClient] = functools.partial(
        _create_us_workbook_cmd_client,
        req_id,
        us_lb_main_base_url,
        dynamic_env_service_accounts,
        tenant
    )
    create_hook = functools.partial(_create_workbook_id_hook, client_factory, dynamic_env_service_accounts, req_id)
    if workbook_mgmt_strategy == CREATE_ONLY:
        return create_hook, _blank_delete_workbook_id_hook
    if workbook_mgmt_strategy == CREATE_AND_DELETE_AFTERWARDS:
        delete_hook = functools.partial(_delete_workbook_id_hook, client_factory)
        return create_hook, delete_hook
    raise Exception(f'Illegal workbook mgmt strategy value: {workbook_mgmt_strategy}')


def _create_us_workbook_cmd_client(
        per_test_request_id: str,
        us_lb_main_base_url: str,
        dynamic_env_service_accounts,
        tenant: TenantDef,
        session: aiohttp.ClientSession,
) -> USWorkbookCommandClient:
    creator_sa = dynamic_env_service_accounts['creator']
    return USWorkbookCommandClient(
        base_url=us_lb_main_base_url,
        tenant=tenant,
        auth_data=IAMAuthData(creator_sa.token),
        req_id=per_test_request_id,
        session=session,
        use_workbooks_api=True,
        read_only=False
    )


async def _blank_create_workbook_id_hook() -> Optional[str]:
    return None


async def _create_workbook_id_hook(
        us_workbook_cmd_client_factory: Callable[[aiohttp.ClientSession], USWorkbookCommandClient],
        service_accounts: dict[str, AccountCredentials],
        req_id: str
) -> Optional[str]:
    async with get_default_aiohttp_session() as session:
        us_workbook_cmd_client = us_workbook_cmd_client_factory(session)
        workbook_title = f'integration-tests-workbook-{req_id}'
        workbook_id = await us_workbook_cmd_client.create_workbook(workbook_title)
        logging.info(f'Workbook with id={workbook_id} created')
        viewers = [service_accounts['viewer-1'], service_accounts['viewer-2']]
        for v in viewers:
            await us_workbook_cmd_client.add_wb_sa_access_binding(workbook_id, v.user_id)
        return workbook_id


async def _blank_delete_workbook_id_hook(
        workbook_id: str
) -> None:
    pass


async def _delete_workbook_id_hook(
        us_workbook_cmd_client_factory: Callable[[aiohttp.ClientSession], USWorkbookCommandClient],
        workbook_id: str
) -> None:
    async with get_default_aiohttp_session() as session:
        us_workbook_cmd_client = us_workbook_cmd_client_factory(session)
        await us_workbook_cmd_client.delete_workbook(workbook_id)
        logging.info(f'Workbook {workbook_id} deleted')

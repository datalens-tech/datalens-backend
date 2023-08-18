from __future__ import annotations

import uuid
import logging
from typing import Any, Dict, List, TYPE_CHECKING, Tuple, Optional, Mapping

import attr
from aiohttp import ClientSession, hdrs
from aiohttp.http import SERVER_SOFTWARE as AIOHTTP_SERVER_SOFTWARE

from bi_constants import api_constants
from bi_cloud_integration.exc import YCBillingAPIError
from bi_cloud_integration.local_metadata import get_yc_service_token_local


if TYPE_CHECKING:
    from aiohttp.web_request import Request
    from bi_cloud_integration.model import Metric

LOGGER = logging.getLogger(__name__)


def make_yc_user_auth_headers(request: Request) -> Dict[str, str]:
    iam_token = request.headers.get(api_constants.DLHeadersCommon.IAM_TOKEN.value)
    if not iam_token:
        LOGGER.warning('iam_token not provided')
        return {}
    return {api_constants.DLHeadersCommon.IAM_TOKEN.value: iam_token}


async def make_yc_service_auth_headers() -> Dict[str, str]:
    iam_token = await get_yc_service_token_local()
    assert iam_token
    return {api_constants.DLHeadersCommon.IAM_TOKEN.value: iam_token}


@attr.s
class YCBillingApiClient:
    yc_billing_host: str = attr.ib()
    auth_headers: Dict[str, str] = attr.ib(kw_only=True)
    user_agent: str = attr.ib(
        kw_only=True,
        default='{}, Datalens'.format(AIOHTTP_SERVER_SOFTWARE)
    )

    @classmethod
    async def create_with_service_auth(cls, **kwargs: Any) -> YCBillingApiClient:
        auth_headers = await make_yc_service_auth_headers()
        return cls(auth_headers=auth_headers, **kwargs)

    @classmethod
    def create_with_user_auth(cls, request: Request, **kwargs: Any) -> YCBillingApiClient:
        auth_headers = make_yc_user_auth_headers(request)
        return cls(auth_headers=auth_headers, **kwargs)

    async def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Mapping[str, str]] = None,
        json_data: Any = None,
    ) -> Any:
        request_id = str(uuid.uuid4())
        headers = {
            'x-request-id': request_id,
            hdrs.USER_AGENT: self.user_agent,
            **self.auth_headers,
        }
        url = f'{self.yc_billing_host}/{path}'
        LOGGER.info('Going to send request to billing api: %s %s, request_id: %s', method, path, request_id)
        async with ClientSession(headers=headers) as session:
            async with session.request(method, url, params=params, json=json_data) as resp:
                if resp.status != 200:
                    body = await resp.text('utf-8', errors='replace')
                    LOGGER.warning(
                        f'Billing API returned non-OK status. '
                        f'Status {resp.status}, '
                        f'YC-API x-request-id: {resp.request_info.headers.get("x-request-id")}, '
                        f'response: {body}'
                    )
                    raise YCBillingAPIError(
                        f'Billing API returned non-OK status: {resp.status}.',
                        status_code=resp.status
                    )

                LOGGER.info('Got %s from billing api', resp.status)
                return await resp.json()

    async def purchases_available(self, folder_id: str, allow_trial_ba: bool = False) -> bool:
        """
        Check whether folder is allowed to purchase anything:
        """
        try:
            answer = await self._make_request(
                'POST',
                'billing/v1/private/billingAccounts:resolve',
                json_data={'folder_id': folder_id},
            )
        except YCBillingAPIError as err:
            if err.status_code == 404:
                return False
            else:
                raise
        assert len(answer['billingAccounts']) <= 1
        if not answer['billingAccounts'] or (
            answer['billingAccounts'][0]['usageStatus'] == 'trial' and
            not allow_trial_ba
        ):
            return False

        return True

    async def get_yc_metric_costs(self, metrics: List[Metric]) -> Tuple[Dict[str, str], str]:
        dry_run_resp = await self._make_request(
            'POST',
            'billing/v1/private/dryRun',
            json_data=dict(metrics=[attr.asdict(m) for m in metrics])
        )
        cost_by_sku = {item['skuId']: item['cost'] for item in dry_run_resp['calculatedItems']}
        for metr in dry_run_resp['unresolvedMetrics']:
            LOGGER.error('Unresolved metric: %s', metr)
        return cost_by_sku, dry_run_resp['currency']

    async def list_subject_billing_accounts(self, iam_uid: str) -> List[str]:
        resp = await self._make_request(
            'GET',
            '/billing/v1/private/billingAccounts:listWithReportsPermission',
            params=dict(iam_uid=iam_uid, subject_type='user_account'),
        )
        # Response format:
        # {
        #     "billingAccounts": [
        #         {
        #             "id": "a6q6s5r2djm5v9feob67",
        #             "name": "yndx.svc.infra"
        #         }
        #     ]
        # }

        return [ba['id'] for ba in resp['billingAccounts']]


# @attr.s
# class YCBillingApiClientFactory:
#     yc_billing_host: str = attr.ib()
#
#     async def create_with_service_auth(self) -> YCBillingApiClient:
#         return await YCBillingApiClient.create_with_service_auth(yc_billing_host=self.yc_billing_host)

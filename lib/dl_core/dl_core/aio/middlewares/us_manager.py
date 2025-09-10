from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import (
    AsyncGenerator,
    Optional,
)

from aiohttp import web
from aiohttp.typedefs import (
    Handler,
    Middleware,
)
import attr

from dl_api_commons.aiohttp import aiohttp_wrappers
from dl_api_commons.tenant_resolver import TenantResolver
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core import exc
from dl_core.aio.aiohttp_wrappers_data_core import DLRequestDataCore
from dl_core.enums import USApiType
from dl_core.services_registry.top_level import DummyServiceRegistry
from dl_core.us_manager.factory import USMFactory
from dl_core.us_manager.us_manager_async import AsyncUSManager
import dl_retrier
from dl_utils.aio import shield_wait_for_complete


LOGGER = logging.getLogger(__name__)


def usm_tenant_resolver_middleware(
    us_base_url: str,
    us_public_token: str,
    crypto_keys_config: CryptoKeysConfig,
    dataset_id_match_info_code: str,
    conn_id_match_info_code: str,
    us_master_token: Optional[str],
    tenant_resolver: TenantResolver,
    ca_data: bytes,
    us_api_type: USApiType,
    retry_policy_factory: dl_retrier.BaseRetryPolicyFactory,
) -> Middleware:
    """
    Middleware fetches dataset or connection from US and picks tenant ID from response

    This middleware works with temp (uncommitted) RCI.
    :param us_base_url:
    :param us_master_token:
    :param us_public_token:
    :param crypto_keys_config:
    :param dataset_id_match_info_code: name of URL match param for dataset ID
    :param conn_id_match_info_code: name of URL match param for connection ID
    :param tenant_resolver:
    :return: Actual middleware
    """
    usm_factory = USMFactory(
        us_base_url=us_base_url,
        crypto_keys_config=crypto_keys_config,
        us_master_token=us_master_token,
        us_public_token=us_public_token,
        retry_policy_factory=retry_policy_factory,
        ca_data=ca_data,
    )

    @web.middleware
    @DLRequestDataCore.use_dl_request
    async def actual_public_usm_workaround_middleware(
        dl_request: DLRequestDataCore, handler: Handler
    ) -> web.StreamResponse:
        if aiohttp_wrappers.RequiredResourceCommon.US_MANAGER not in dl_request.required_resources:
            return await handler(dl_request.request)

        rci = dl_request.temp_rci

        # Fetching dataset or connection & determining tenant id
        # Here we know that we need US manager for request
        entry_id = next(
            dl_request.request.match_info[match_info_code]
            for match_info_code in (dataset_id_match_info_code, conn_id_match_info_code)
            if match_info_code in dl_request.request.match_info
        )

        sr = dl_request.services_registry
        if sr is None:
            # It won't really be needed for anything.
            # We just need to initialize a USM and fetch an entry.
            # Might be used in pre/post-init hooks though.
            # But we expect nothing serious - it just won't work with the dummy SR otherwise
            sr = DummyServiceRegistry(rci=rci)
        assert sr is not None

        usm = usm_factory.get_async_usm(rci=rci, services_registry=sr, us_api_type=us_api_type)
        try:
            try:
                entry = await usm.get_by_id(entry_id)
            except exc.USObjectNotFoundException as e:
                raise web.HTTPNotFound() from e
            else:
                tenant = tenant_resolver.resolve_tenant_def_by_tenant_id(entry.raw_tenant_id)
        finally:
            await usm.close()

        rci = attr.evolve(rci, tenant=tenant)
        dl_request.replace_temp_rci(rci)

        return await handler(dl_request.request)

    return actual_public_usm_workaround_middleware


@contextlib.asynccontextmanager
async def _usm_close_cm(usm: AsyncUSManager, label: str) -> AsyncGenerator[None, None]:
    try:
        yield
    finally:
        try:
            await shield_wait_for_complete(usm.close())
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa
            LOGGER.warning(f"Error during closing {label} USManager", exc_info=True)


def us_manager_middleware(
    us_base_url: str,
    crypto_keys_config: CryptoKeysConfig,
    ca_data: bytes,
    retry_policy_factory: dl_retrier.BaseRetryPolicyFactory,
    embed: bool = False,
) -> Middleware:
    usm_factory = USMFactory(
        us_base_url=us_base_url,
        crypto_keys_config=crypto_keys_config,
        ca_data=ca_data,
        retry_policy_factory=retry_policy_factory,
    )

    @web.middleware
    @DLRequestDataCore.use_dl_request
    async def actual_us_manager_middleware(dl_request: DLRequestDataCore, handler: Handler) -> web.StreamResponse:
        if aiohttp_wrappers.RequiredResourceCommon.US_MANAGER not in dl_request.required_resources:
            return await handler(dl_request.request)

        if embed:
            usm = usm_factory.get_embed_async_usm(
                rci=dl_request.rci,
                services_registry=dl_request.services_registry,
            )
        else:
            usm = usm_factory.get_regular_async_usm(
                rci=dl_request.rci,
                services_registry=dl_request.services_registry,
            )

        async with _usm_close_cm(usm, label="user"):
            dl_request.us_manager = usm
            return await handler(dl_request.request)

    return actual_us_manager_middleware


def public_us_manager_middleware(
    us_base_url: str,
    us_public_token: str,
    crypto_keys_config: CryptoKeysConfig,
    ca_data: bytes,
    retry_policy_factory: dl_retrier.BaseRetryPolicyFactory,
) -> Middleware:
    """
    Middleware to create public US manager. Works with committed RCI.
    Must be used after `public_usm_workaround_middleware`.
    Can not be used together with `us_manager_middleware` because public USM treated as user USM.
    """
    usm_factory = USMFactory(
        us_base_url=us_base_url,
        crypto_keys_config=crypto_keys_config,
        us_public_token=us_public_token,
        ca_data=ca_data,
        retry_policy_factory=retry_policy_factory,
    )

    @web.middleware
    @DLRequestDataCore.use_dl_request
    async def actual_public_us_manager_middleware(
        dl_request: DLRequestDataCore, handler: Handler
    ) -> web.StreamResponse:
        if aiohttp_wrappers.RequiredResourceCommon.US_MANAGER not in dl_request.required_resources:
            return await handler(dl_request.request)

        usm = usm_factory.get_public_async_usm(
            rci=dl_request.rci,
            services_registry=dl_request.services_registry,
        )
        async with _usm_close_cm(usm, label="public"):
            dl_request.us_manager = usm
            return await handler(dl_request.request)

    return actual_public_us_manager_middleware


def service_us_manager_middleware(
    us_base_url: str,
    us_master_token: str,
    crypto_keys_config: CryptoKeysConfig,
    ca_data: bytes,
    retry_policy_factory: dl_retrier.BaseRetryPolicyFactory,
    as_user_usm: bool = False,
) -> Middleware:
    usm_factory = USMFactory(
        us_base_url=us_base_url,
        crypto_keys_config=crypto_keys_config,
        us_master_token=us_master_token,
        ca_data=ca_data,
        retry_policy_factory=retry_policy_factory,
    )

    @web.middleware
    @DLRequestDataCore.use_dl_request
    async def actual_service_us_manager_middleware(
        dl_request: DLRequestDataCore, handler: Handler
    ) -> web.StreamResponse:
        target_resource = (
            aiohttp_wrappers.RequiredResourceCommon.US_MANAGER
            if as_user_usm
            else aiohttp_wrappers.RequiredResourceCommon.SERVICE_US_MANAGER
        )

        if target_resource not in dl_request.required_resources:
            return await handler(dl_request.request)

        usm = usm_factory.get_master_async_usm(
            rci=dl_request.rci,
            services_registry=dl_request.services_registry,
        )

        async with _usm_close_cm(usm, label="service"):
            if not as_user_usm:
                dl_request.service_us_manager = usm
            else:
                # For tests only!!!
                LOGGER.info("Setting service USM as user USM")
                dl_request.us_manager = usm

            return await handler(dl_request.request)

    return actual_service_us_manager_middleware

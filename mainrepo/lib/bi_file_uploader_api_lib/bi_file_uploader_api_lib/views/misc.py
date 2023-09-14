from __future__ import annotations

import logging
from typing import ClassVar

from aiohttp import web

from bi_api_commons.aiohttp.aiohttp_wrappers import (
    RequiredResource,
    RequiredResourceCommon,
)
from bi_file_uploader_api_lib.schemas import misc as misc_schemas
from bi_file_uploader_api_lib.views.base import FileUploaderBaseView
from bi_file_uploader_lib.enums import RenameTenantStatus
from bi_file_uploader_lib.redis_model.models import RenameTenantStatusModel
from bi_file_uploader_task_interface.tasks import (
    CleanupTenantFilePreviewsTask,
    CleanupTenantTask,
    RenameTenantFilesTask,
)

LOGGER = logging.getLogger(__name__)


class CleanupTenantView(FileUploaderBaseView):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset(
        {
            RequiredResourceCommon.SKIP_AUTH,
            RequiredResourceCommon.SKIP_CSRF,
            RequiredResourceCommon.MASTER_KEY,
        }
    )

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(misc_schemas.CleanupRequestSchema)
        tenant_id = req_data["tenant_id"]

        task_processor = self.dl_request.get_task_processor()
        await task_processor.schedule(
            CleanupTenantTask(
                tenant_id=tenant_id,
            )
        )
        LOGGER.info(f"Scheduled CleanupTenantTask for tenant id {tenant_id}")

        await task_processor.schedule(
            CleanupTenantFilePreviewsTask(
                tenant_id=tenant_id,
            )
        )
        LOGGER.info(f"Scheduled CleanupTenantFilePreviewsTask for tenant id {tenant_id}")

        return web.Response()


class RenameTenantFilesView(FileUploaderBaseView):
    # TODO: Delete this API handler (and task) after migration to organizations

    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset(
        {
            RequiredResourceCommon.SKIP_AUTH,
            RequiredResourceCommon.SKIP_CSRF,
            RequiredResourceCommon.MASTER_KEY,
        }
    )

    async def post(self) -> web.StreamResponse:
        req_data = await self._load_post_request_schema_data(misc_schemas.RenameFilesRequestSchema)
        tenant_id = req_data["tenant_id"]

        rmm = self.dl_request.get_redis_model_manager()
        await RenameTenantStatusModel(manager=rmm, id=tenant_id, status=RenameTenantStatus.scheduled).save()

        task_processor = self.dl_request.get_task_processor()
        await task_processor.schedule(RenameTenantFilesTask(tenant_id=tenant_id))
        LOGGER.info(f"Scheduled RenameTenantFilesTask for tenant_id {tenant_id}")

        return web.Response()

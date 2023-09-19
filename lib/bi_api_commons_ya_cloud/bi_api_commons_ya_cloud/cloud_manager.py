from __future__ import annotations

import asyncio
from typing import (
    ClassVar,
    Optional,
    Sequence,
)

import attr

from bi_api_commons_ya_cloud.models import (
    TenantDCProject,
    TenantYCFolder,
    TenantYCOrganization,
)
from bi_cloud_integration.iam_rm_client import DLFolderServiceClient
from bi_cloud_integration.yc_subjects import DLYCMSClient
from dl_api_commons.base_models import TenantDef
from dl_utils.aio import alist


DEFAULT_DOMAIN = "yandex.ru"


@attr.s
class SubjectInfo:
    subj_id: str = attr.ib()
    email: Optional[str] = attr.ib()
    subj_type: str = attr.ib()


@attr.s
class CloudManagerAPI:
    DEFAULT_DOMAIN: ClassVar[str] = DEFAULT_DOMAIN
    _TYPE_NAME_TO_RLS_TYPE: ClassVar[dict[str, str]] = {
        "SUBJECT_TYPE_UNSPECIFIED": "unknown",
        "USER_ACCOUNT": "user",
        "SERVICE_ACCOUNT": "user",
        "GROUP": "group",
    }

    _tenant: TenantDef = attr.ib()
    _yc_fs_cli: DLFolderServiceClient = attr.ib()
    _yc_ms_cli: DLYCMSClient = attr.ib()

    async def folder_id_to_cloud_id(self, yacloud_folder_id: str) -> str:
        return await self._yc_fs_cli.resolve_folder_id_to_cloud_id(yacloud_folder_id)

    async def _resolve_tenant_info_kwargs(self) -> dict:
        if isinstance(self._tenant, TenantYCOrganization):
            # without org_ prefix
            return dict(org_id=self._tenant.org_id)
        if isinstance(self._tenant, TenantYCFolder):
            cloud_id = await self.folder_id_to_cloud_id(self._tenant.folder_id)
            return dict(cloud_id=cloud_id)
        if isinstance(self._tenant, TenantDCProject):
            return dict(cloud_id=self._tenant.get_tenant_id())
        raise Exception("Unexpected tenant_info", self._tenant)

    async def subject_emails_to_infos(
        self,
        subject_emails: Sequence[str],
    ) -> dict[str, SubjectInfo]:
        tenant_kwargs = await self._resolve_tenant_info_kwargs()
        emails = set(subject_emails)
        subject_chunks = await asyncio.gather(
            *(
                alist(
                    self._yc_ms_cli.list_members_by_emails(
                        filter=query,
                        emails=emails,
                        **tenant_kwargs,
                    )
                )
                for query in self._yc_ms_cli.make_email_queries_generator(emails)
            )
        )
        subject_to_info: dict[str, SubjectInfo] = {
            subj.email
            or "": SubjectInfo(
                email=subj.email,
                subj_id=subj.id,
                subj_type=self._TYPE_NAME_TO_RLS_TYPE[subj.subject_type],
            )
            for query_chunk in subject_chunks
            for page_chunk in query_chunk
            for subj in page_chunk.values()
        }
        return subject_to_info

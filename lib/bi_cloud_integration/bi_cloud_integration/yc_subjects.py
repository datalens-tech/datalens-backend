from __future__ import annotations

from typing import AsyncGenerator, ClassVar, Dict, Optional, Sequence, Iterable

import attr

from yandex.cloud.priv.iam.v1.console import membership_service_pb2, membership_service_pb2_grpc
from yandex.cloud.priv.oauth import claims_pb2
from yandex.cloud.priv.oauth.v1 import claim_service_pb2, claim_service_pb2_grpc

from bi_cloud_integration.yc_client_base import DLYCSingleServiceClient

from bi_utils.utils import join_in_chunks


@attr.s(auto_attribs=True, frozen=True)
class SubjectInfo:
    id: str
    subject_type: str  # "SUBJECT_TYPE_UNSPECIFIED" | "USER_ACCOUNT" | "SERVICE_ACCOUNT" | "GROUP"

    email: Optional[str] = None
    title: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    short_title: Optional[str] = None  # SubjectClaims.preferred_username
    avatar: Optional[str] = None
    avatar_data: Optional[str] = attr.ib(default=None, repr=False)
    timezone: Optional[str] = None
    locale: Optional[str] = None
    phone_number: Optional[str] = None
    federation_id: Optional[str] = None
    federation_name: Optional[str] = None
    yateam_staff_login: Optional[str] = None
    yandex_passport_uid: Optional[int] = None
    yandex_passport_login: Optional[str] = None
    yandex_avatar_id: Optional[str] = None
    yandex_email: Optional[str] = None
    yandex_two_factor_authentication_enabled: Optional[bool] = None

    def as_dict(self) -> dict:
        return attr.asdict(self)


def subject_claims_to_info(
        sc: claims_pb2.SubjectClaims,  # type: ignore  # TODO: fix
) -> SubjectInfo:
    return SubjectInfo(
        id=sc.sub,  # type: ignore  # TODO: fix
        title=sc.name or None,  # type: ignore  # TODO: fix
        given_name=sc.given_name or None,  # type: ignore  # TODO: fix
        family_name=sc.family_name or None,  # type: ignore  # TODO: fix
        short_title=sc.preferred_username or None,  # type: ignore  # TODO: fix
        avatar=sc.picture or None,  # type: ignore  # TODO: fix
        avatar_data=sc.picture_data or None,  # type: ignore  # TODO: fix
        # The RP MUST NOT rely upon this value being unique, as discussed in Section 5.7.
        email=sc.email or None,  # type: ignore  # TODO: fix
        timezone=sc.zoneinfo or None,  # type: ignore  # TODO: fix
        locale=sc.locale or None,  # type: ignore  # TODO: fix
        phone_number=sc.phone_number or None,  # type: ignore  # TODO: fix
        federation_id=sc.federation.id or None,  # type: ignore  # TODO: fix
        federation_name=sc.federation.name or None,  # type: ignore  # TODO: fix

        subject_type=claims_pb2.SubjectType.Name(sc.sub_type),  # type: ignore  # TODO: fix
        yateam_staff_login=sc.yandex_claims.staff_login or None,  # type: ignore  # TODO: fix
        yandex_passport_uid=sc.yandex_claims.passport_uid or None,  # type: ignore  # TODO: fix
        yandex_passport_login=sc.yandex_claims.login or None,  # type: ignore  # TODO: fix
        yandex_avatar_id=sc.yandex_claims.avatar_id or None,  # type: ignore  # TODO: fix
        yandex_email=sc.yandex_claims.email or None,  # type: ignore  # TODO: fix
        # Unfortunately, this uses `False` for 'not applicable' too:
        yandex_two_factor_authentication_enabled=(
            sc.yandex_claims.two_factor_authentication_enabled or None  # type: ignore  # TODO: fix
        ),
    )


class DLYCCSClient(DLYCSingleServiceClient):
    # endpoint: YC_SS_ENDPOINT
    service_cls = claim_service_pb2_grpc.ClaimServiceStub

    async def get_subjects_details(self, subject_ids: Sequence[str]) -> Dict[str, SubjectInfo]:
        req = claim_service_pb2.GetClaimsRequest(subject_ids=subject_ids)
        resp = await self.service.Get.aio(req)
        subject_datas = [subject_claims_to_info(subj.subject_claims) for subj in resp.subject_details]
        return {subj.id: subj for subj in subject_datas}


class DLYCMSClient(DLYCSingleServiceClient):
    # endpoint: YC_API_ENDPOINT_IAM
    service_cls = membership_service_pb2_grpc.MembershipServiceStub

    FILTER_OR: ClassVar[str] = ' OR '
    FILTER_AND: ClassVar[str] = ' AND '
    DEFAULT_SUBJECT_TYPE_FILTER: ClassVar[str] = 'claims.subType = "USER_ACCOUNT"'
    MAX_FILTER_LENGTH: ClassVar[int] = 1000
    # Might be useful later:
    # DEFAULT_SUBJECT_TYPE_FILTER: ClassVar[str] = 'claims.subType IN ("USER_ACCOUNT", "SERVICE_ACCOUNT")'

    @staticmethod
    def quote_filter_string(value: str, quote: str = '"') -> str:
        value_q = value
        value_q = value_q.replace('\r', ' ').replace('\n', ' ')
        value_q = value_q.replace(quote, quote + quote)
        return quote + value_q + quote

    @classmethod
    def make_filter_or(cls, *items: str) -> str:
        return cls.FILTER_OR.join(f'({expr})' for expr in items)

    @classmethod
    def make_filter_and(cls, *items: str) -> str:
        return cls.FILTER_AND.join(f'({expr})' for expr in items)

    @classmethod
    def make_members_filter(cls, search_text: str, default_type_filter: bool = True) -> str:
        search_text_q = cls.quote_filter_string(search_text)
        query = cls.make_filter_or(
            f"claims.sub = {search_text_q}",
            f"claims.name ICONTAINS {search_text_q}",
            f"claims.preferredUsername ICONTAINS {search_text_q}",
            f"claims.email ICONTAINS {search_text_q}",
        )
        if default_type_filter:
            query = cls.make_filter_and(
                query,
                cls.DEFAULT_SUBJECT_TYPE_FILTER
            )
        return query

    @classmethod
    def make_email_queries_generator(cls, emails: Iterable[str]) -> Iterable[str]:
        return join_in_chunks(
            (
                f"(claims.email ICONTAINS {cls.quote_filter_string(email)})"
                for email in sorted(emails)
            ),
            sep=cls.FILTER_OR,
            max_len=cls.MAX_FILTER_LENGTH,
        )

    async def list_members_by_emails(
        self,
        cloud_id: str = '',
        org_id: str = '',
        emails: Optional[set[str]] = None,
        filter: str = '',
        max_pages: int = 9000,
        page_size: int = 1000,
    ) -> AsyncGenerator[Dict[str, SubjectInfo], None]:
        if emails is None:
            emails = set()
        async for chunk in self.list_members(
            cloud_id=cloud_id,
            org_id=org_id,
            filter=filter,
            max_pages=max_pages,
            page_size=page_size,
        ):
            # we check emails by ICONTAINS statement
            # so we have to doublecheck the result
            yield {subj_id: subj for subj_id, subj in chunk.items() if subj.email in emails}

    async def list_members(
            self, cloud_id: str = '', org_id: str = '', filter: str = '', max_pages: int = 9000,
            page_size: int = 1000,
    ) -> AsyncGenerator[Dict[str, SubjectInfo], None]:
        if not (cloud_id or org_id):
            raise TypeError("Either cloud_id or org_id should be nonempty")
        if cloud_id and org_id:
            raise TypeError("Only one of (cloud_id, org_id) should be nonempty")
        page_token = ''
        for page in range(max_pages):
            req = membership_service_pb2.ListMembersRequest(
                # Subjects listing filter. Filter specs:
                # https://wiki.yandex-team.ru/users/zdazzy/cloud/iam/service/console/objectfilter/
                filter=filter,
                page_size=page_size,
                page_token=page_token,
            )
            if cloud_id:
                req.cloud_id = cloud_id
            else:
                req.organization_id = org_id
            resp = await self.service.ListMembers.aio(req)
            subject_datas = [
                subject_claims_to_info(subj.subject_claims)
                for subj in resp.members
            ]
            yield {subj.id: subj for subj in subject_datas}
            if not resp.next_page_token:
                return
            page_token = resp.next_page_token
        raise Exception(f"Too many pages in IAM list_cloud_members {cloud_id=!r}")

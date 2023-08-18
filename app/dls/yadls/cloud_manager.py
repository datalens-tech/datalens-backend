from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
import uuid
import time
from typing import TYPE_CHECKING, Any, ClassVar, Generator, Optional, Sequence, Type, TypeVar

from bi_cloud_integration import exc as bi_yc_exc
from bi_cloud_integration.iam_rm_client import DLFolderServiceClient
from bi_cloud_integration.local_metadata import get_yc_service_token_local
from bi_cloud_integration.yc_subjects import DLYCCSClient, DLYCMSClient
from bi_cloud_integration.yc_ts_client import get_yc_service_token
from bi_utils.aio import alist, await_sync

from . import db as db_base
from .db_utils import get_or_create_aio, get_or_create_sasess
from .exceptions import UserError
from .utils import get_default_requests_session, make_uuid, req as req_base

if TYPE_CHECKING:
    from bi_cloud_integration.yc_subjects import SubjectInfo


LOG = logging.getLogger(__name__)


DEFAULT_DOMAIN = 'yandex.ru'


def login_to_email(login: str, default_domain: str = DEFAULT_DOMAIN) -> str:
    """ Compatibility to-be-deprecated mapping """
    if '@' in login:
        return login
    return f'{login}@{default_domain}'


def email_to_login(email: Optional[str], default_domain: str = DEFAULT_DOMAIN) -> Optional[str]:
    """ Compatibility to-be-deprecated mapping """
    if not email:
        return None
    pieces = email.rsplit('@', 1)
    if len(pieces) != 2:
        return None
    login, domain = pieces
    if domain == default_domain:
        return login
    return email


async def get_yc_token(timeout: float = 32.0) -> str:
    from .settings import settings
    if settings.YC_KEY_DATA_B:
        env_config = settings.ENV_CONFIG
        assert env_config is not None
        return await get_yc_service_token(
            key_data=settings.YC_KEY_DATA_B,
            yc_ts_endpoint=env_config.YC_TS_ENDPOINT,
            timeout=timeout,
        )

    return await get_yc_service_token_local()


_CM_TV = TypeVar('_CM_TV', bound='CloudManagerBase')


class CloudManagerBase:

    _yc_token_cached: Optional[str] = None
    _yc_ms_cli: Optional[DLYCMSClient] = None

    def __init__(self, request_id: Optional[str] = None):
        self.request_id = request_id
        self.request_id_norm = DLYCMSClient.normalize_request_id(request_id)

    async def __aenter__(self: _CM_TV) -> _CM_TV:
        return self

    async def __aexit__(self, *args: Any) -> None:
        ms_cli = self._yc_ms_cli
        if ms_cli is not None:
            self._yc_ms_cli = None
            ms_cli.close()

    async def get_yc_token(self) -> str:
        token = self._yc_token_cached
        if token is None:
            token = await get_yc_token()
            self._yc_token_cached = token
        return token

    async def get_yc_ms_cli(self, token: str) -> DLYCMSClient:
        cli = self._yc_ms_cli
        if cli is None:
            from .settings import settings
            env_config = settings.ENV_CONFIG
            assert env_config is not None
            cli = DLYCMSClient.create(
                endpoint=env_config.YC_API_ENDPOINT_IAM,
                bearer_token=token,
                request_id=self.request_id_norm,
            )
            self._yc_ms_cli = cli
        return cli


class CloudManagerAPI(CloudManagerBase):

    DEFAULT_DOMAIN: ClassVar[str] = DEFAULT_DOMAIN
    _TYPE_NAME_TO_DLS_TYPE: ClassVar[dict[str, str]] = {
        'SUBJECT_TYPE_UNSPECIFIED': 'unknown',
        'USER_ACCOUNT': 'user',
        'SERVICE_ACCOUNT': 'user',
        'GROUP': 'group',
    }

    _yc_cs_cli: Optional[DLYCCSClient] = None
    _yc_fs_cli: Optional[DLFolderServiceClient] = None

    async def __aexit__(self, *args: Any) -> None:
        try:
            try:
                cs_cli = self._yc_cs_cli
                if cs_cli is not None:
                    self._yc_cs_cli = None
                    cs_cli.close()
            finally:
                fs_cli = self._yc_fs_cli
                if fs_cli is not None:
                    self._yc_fs_cli = None
                    fs_cli.close()
        finally:
            await super().__aexit__(*args)

    async def get_yc_fs_cli(self, token: str) -> DLFolderServiceClient:
        cli = self._yc_fs_cli
        if cli is None:
            from .settings import settings
            env_config = settings.ENV_CONFIG
            assert env_config is not None
            cli = DLFolderServiceClient.create(
                endpoint=env_config.YC_API_ENDPOINT_RM,
                bearer_token=token,
                request_id=self.request_id_norm,
            )
            self._yc_fs_cli = cli
        return cli

    async def get_yc_cs_cli(self, token: str) -> DLYCCSClient:
        cli = self._yc_cs_cli
        if cli is None:
            from .settings import settings
            env_config = settings.ENV_CONFIG
            assert env_config is not None
            cli = DLYCCSClient.create(
                endpoint=env_config.YC_SS_ENDPOINT,
                bearer_token=token,
                request_id=self.request_id_norm,
            )
            self._yc_cs_cli = cli
        return cli

    @classmethod
    def subject_info_to_dls_data(cls, subj: SubjectInfo) -> dict[str, Any]:
        login = email_to_login(subj.email)
        subj_type = cls._TYPE_NAME_TO_DLS_TYPE[subj.subject_type]
        # Reference:
        # https://github.yandex-team.ru/data-ui/cloud-components/blob/1a44aa898cb9632ecc10e53d2547d033c860c24c/src/components/organization/OrganizationUserCard/getUserName.ts#L8
        title = (
            subj.title  # SubjectClaims.name
            or f"{subj.given_name or ''} {subj.family_name or ''}".strip()
            or subj.short_title  # SubjectClaims.preferred_username
            or subj.yandex_passport_login  # SubjectClaims.yandex_claims.login
            # or subj.email  # Not in the data-ui logic
            or subj.id  # SubjectClaims.sub
            or ""
        )
        return dict(
            __rlsid=login,
            __source="iam",
            cloud_icon=subj.avatar,
            cloud_icon_data=subj.avatar_data,
            cloud_user_id=subj.id,
            cloud_user_login=login,
            name=f"{subj_type}:{subj.id}",
            title=title,
            type=subj_type,
        )

    @contextlib.contextmanager
    def yc_exc_wrapper(self, is_user_req: bool) -> Generator[None, None, None]:
        try:
            yield
        except (bi_yc_exc.YCUnauthenticated, bi_yc_exc.YCPermissionDenied) as err:
            if not is_user_req:
                raise
            raise UserError(
                dict(message=err.info.internal_details or "IAM API Auth/Access Error"),
                status_code=403,
            ) from err

    async def folder_id_to_cloud_id(self, yacloud_folder_id: str) -> str:
        iam_token = await self.get_yc_token()
        yc_fs_cli = await self.get_yc_fs_cli(token=iam_token)
        return await yc_fs_cli.resolve_folder_id_to_cloud_id(yacloud_folder_id)

    async def _resolve_tenant_info_kwargs(self, tenant_info: dict) -> dict:
        org_id = tenant_info.get("org_id")
        if org_id:
            return dict(org_id=org_id)

        cloud_id = tenant_info.get("cloud_id")
        if cloud_id:
            return dict(cloud_id=cloud_id)

        folder_id = tenant_info.get("folder_id")
        if folder_id:
            cloud_id = await self.folder_id_to_cloud_id(folder_id)
            return dict(cloud_id=cloud_id)

        raise Exception("Unexpected tenant_info", tenant_info)

    async def suggest_subjects(
            self, search_text: str, tenant_info: dict, iam_token: Optional[str],
            *, limit: int = 5,
    ) -> Sequence[SubjectInfo]:
        if limit >= 1000:
            raise UserError(dict(message="Not allowed to request limit >= 1000"))
        is_user_req = iam_token is not None
        if iam_token is None:
            # Common note: allowing explicit `iam_token=None`, but not allowing
            # to omit it entirely on the call.
            iam_token = await self.get_yc_token()
        yc_ms_cli = await self.get_yc_ms_cli(token=iam_token)
        query = yc_ms_cli.make_members_filter(search_text)
        if len(query) >= 1000:
            raise UserError(dict(message="Search query is too long"))
        tenant_kwargs = await self._resolve_tenant_info_kwargs(tenant_info)
        with self.yc_exc_wrapper(is_user_req=is_user_req):
            pages_aiter = yc_ms_cli.list_members(
                filter=query,
                page_size=limit,
                **tenant_kwargs,
            )
            subjects: list[SubjectInfo] = []
            async for result_page in pages_aiter:
                subjects.extend(result_page.values())
                break  # take only first page
            await pages_aiter.aclose()
        return subjects

    async def subject_ids_to_infos(self, subject_ids: Sequence[str], iam_token: Optional[str]) -> dict[str, SubjectInfo]:
        is_user_req = iam_token is not None
        if iam_token is None:
            iam_token = await self.get_yc_token()
        yc_cs_cli = await self.get_yc_cs_cli(token=iam_token)
        with self.yc_exc_wrapper(is_user_req=is_user_req):
            start = time.monotonic()
            data = await yc_cs_cli.get_subjects_details(subject_ids)
            duration = time.monotonic() - start
            LOG.info('yc_cs_cli.get_subjects_details duration: %s', duration)
            return data

    async def subject_logins_to_infos(
            self, subject_logins: Sequence[str], tenant_info: dict, iam_token: Optional[str],
    ) -> tuple[dict[str, SubjectInfo], dict[str, Any]]:
        if iam_token is None:
            iam_token = await self.get_yc_token()
        yc_ms_cli = await self.get_yc_ms_cli(token=iam_token)
        tenant_kwargs = await self._resolve_tenant_info_kwargs(tenant_info)
        emails = map(login_to_email, subject_logins)
        subject_chunks = await asyncio.gather(*(
            alist(yc_ms_cli.list_members(
                filter=query,
                **tenant_kwargs,
            ))
            for query in yc_ms_cli.make_email_queries_generator(emails)
        ))
        subject_to_info: dict[str, SubjectInfo] = {
            email_to_login(subj.email) or "": subj
            for query_chunk in subject_chunks
            for page_chunk in query_chunk
            for subj in page_chunk.values()
        }
        details = dict(tenant_info=tenant_info)
        return subject_to_info, details


class CloudManagerIAMImport(CloudManagerBase):
    """ Everything common to the full-IAM-synchronization use-case """
    key: ClassVar[str] = 'cfg__clouds'
    Data: ClassVar[Type[db_base.Data]] = db_base.Data


class CloudManagerCFG(CloudManagerIAMImport):
    """ Helper for handling API requests for adding/removing IAM-synchronization requirements (list of `cloud_id`s) """

    def __init__(self, main_manager=None, **kwargs):
        self.main_manager = main_manager
        super().__init__(**kwargs)

    @contextlib.asynccontextmanager
    async def cfg(self, for_update=True):
        db_mgr = self.main_manager.db
        model = self.Data
        stmt = model.select_(model.key == self.key)
        async with db_mgr.manage(writing=True, tx=True) as db_conn:
            obj = await get_or_create_aio(
                conn=db_conn,
                table=model,
                key=['key'],
                values=dict(key=self.key, data='', meta={}),
                for_update=for_update,
            )
            obj_orig = copy.deepcopy(dict(obj))
            yield obj
            if any(obj[key] != obj_orig[key] for key in ('data', 'meta')):
                stmt = (
                    model.update_()
                    .where(model.key == self.key)
                    .values(dict(data=obj.data, meta=obj.meta)))
                upd_res = await db_conn.execute(stmt)
                LOG.info("update result: %r", upd_res.rowcount)

    async def get_clouds_info(self):
        async with self.cfg(for_update=False) as obj:  # pylint: disable=not-async-context-manager
            return obj.meta

    async def add_cloud(self, identifier, meta=None):
        async with self.cfg(for_update=True) as obj:  # pylint: disable=not-async-context-manager
            info = obj.meta.setdefault('clouds', {}).setdefault(identifier, {})
            info.update(enabled=1, meta=meta)
            return info

    async def remove_cloud(self, identifier, meta=None):
        async with self.cfg(for_update=True) as obj:  # pylint: disable=not-async-context-manager
            info = obj.meta.setdefault('clouds', {}).pop(identifier, None)
            if info is not None:
                obj.meta.setdefault('clouds_disabled', {})[identifier] = dict(info, remove_meta=meta)
            return info


class CloudManagerSync(CloudManagerIAMImport):
    """ Actual IAM-to-db synchronous synchronization """

    db_get_session = staticmethod(db_base.get_session)
    Subject: ClassVar[Type[db_base.Subject]] = db_base.Subject
    NodeConfig: ClassVar[Type[db_base.NodeConfig]] = db_base.NodeConfig
    group_members_m2m: ClassVar[Type[db_base.group_members_m2m]] = db_base.group_members_m2m

    _yc_request_id: Optional[str] = None
    db_engine = None
    db_conn = None
    db_session: Optional[db_base.TDBSession] = None

    def __init__(self):
        self.requests_session = get_default_requests_session()

    def get_yc_request_id(self) -> str:
        reqid = self._yc_request_id
        if reqid is None:
            reqid = str(uuid.uuid4())
            LOG.info("IAM Request Id: %r", reqid)
            self._yc_request_id = reqid
        return reqid

    def req_iam_sync(self, *args, timeout=32, **kwargs):
        from .settings import settings
        headers = dict(kwargs.pop('headers', None) or {})
        headers['X-YaCloud-SubjectToken'] = await_sync(self.get_yc_token())
        headers['X-Request-Id'] = self.get_yc_request_id()
        kwargs.setdefault('session', self.requests_session)
        return req_base(
            *args,
            headers=headers,
            base_url=settings.YC_IAM_LEGACY_HOST,
            timeout=timeout,
            **kwargs)

    def _iterate_iam_pages_sync(self, uri, params=None, req=None, page_size=1000, max_pages=9000, **req_kwargs):
        if req is None:
            req = self.req_iam_sync
        if params is None:
            params = {}

        page_token = None
        page = None
        for page in range(max_pages):
            page_params = params.copy()
            page_params['pageSize'] = page_size
            if page_token:
                page_params['pageToken'] = page_token
            resp = req(uri, params=page_params, **req_kwargs)
            resp_data = resp.json()
            yield resp, resp_data
            page_token = resp_data.get('nextPageToken', None)
            if page_token is None:
                return
        raise Exception("Too many pages in IAM: uri={!r}, page={!r}".format(uri, page))

    def _run_sync_import_i(
            self,
            dbsess: db_base.TDBSession,
            run_subjects_manager: bool = True,
            check_known_clouds: bool = False,
            gather_folderids: bool = False,
    ) -> None:
        Data = self.Data
        Subject = self.Subject
        NodeConfig = self.NodeConfig
        membership = self.group_members_m2m

        assert dbsess is not None
        try:
            conf = (
                dbsess.query(Data)
                .filter_by(key=self.key)
                .one())
        except db_base.NoResultFound:
            LOG.info("Configuration does not exist (at `Data(key=%r)`).", self.key)
            return
        LOG.info("clouds config: %r", conf.meta)
        clouds = conf.meta.get('clouds', {})
        cloud_ids = list(clouds.keys())

        if check_known_clouds:
            cloudses = self._iterate_iam_pages_sync('/v1/allClouds')
            known_clouds_it = (
                item
                for _, resp_data in cloudses
                for item in resp_data['result'])
            known_clouds = list(known_clouds_it)
            known_cloud_ids = set(item['id'] for item in known_clouds)

            unknown_cloud_ids = set(cloud_ids) - known_cloud_ids
            if unknown_cloud_ids:
                LOG.error("Unknown clouds configured (not in IAM /v1/allClouds): %r", sorted(unknown_cloud_ids))
                known_cloud_names = {item['id']: item['name'] for item in known_clouds}
                LOG.info("Known clouds: %r", known_cloud_names)

        uid_to_user: dict[str, dict] = {}
        # actual_cids = []
        actual_sources = []
        # users = []
        # uid_to_cids = {}

        iam_token = await_sync(self.get_yc_token())
        yc_ms_cli = await_sync(self.get_yc_ms_cli(token=iam_token))
        for cid in cloud_ids:
            cloud_source = 'cloud__{}'.format(cid)
            try:
                if gather_folderids:
                    cloud_folderses = self._iterate_iam_pages_sync('/v1/allFolders', params=dict(cloudId=cid))
                    cloud_folders = (
                        item
                        for _, resp_data in cloud_folderses
                        for item in resp_data['result'])
                    # cloud_folders = list(cloud_folders)
                    cloud_folderids = [item['id'] for item in cloud_folders]

                # TODO: Use
                # https://bb.yandex-team.ru/projects/CLOUD/repos/cloud-go/browse/private-api/yandex/cloud/priv/organizationmanager/v1/user_service.proto#19
                # instead, if this happens to still be necessary.
                cloud_subjectses = await_sync(alist(yc_ms_cli.list_members(
                    cloud_id=cid,
                    filter=yc_ms_cli.DEFAULT_SUBJECT_TYPE_FILTER,
                )))
                cloud_subjects = [subject for subjects_map in cloud_subjectses for subject in subjects_map.values()]
                if not cloud_subjects:
                    LOG.error("Suspiciously empty cloud: %r (%r).", cid, clouds[cid])
                    continue  # will not be added to 'actual_sources'
                LOG.info("yacloud subjects count: %s -> %s", cid, len(cloud_subjects))

                for subject_info in cloud_subjects:
                    uid = subject_info.id
                    login = email_to_login(subject_info.email)

                    user_data = uid_to_user.get(uid)
                    if user_data is None:
                        user_data = dict(
                            kind='user',
                            name=f'user:{uid}',  # TODO?: `svcacc:{uid}`
                            active=True,
                            # XXXX/WARNING: essentially, only adds one of the user's clouds.
                            source=cloud_source,
                            meta=dict(
                                _id=uid,
                                __yacloud_data=subject_info.as_dict(),
                                __rlsid=login,
                                _login=login,
                                title=subject_info.title,
                                avatar=subject_info.avatar,
                            ))
                        uid_to_user[uid] = user_data

                    user_data['meta'].setdefault('__yacloud_ids', []).append(cid)
                    if gather_folderids:
                        user_data['meta'].setdefault('__yacloud_folder_ids', []).extend(cloud_folderids)

            except Exception as exc:  # pylint: disable=broad-except
                resp = getattr(exc, 'response', None)
                resp = getattr(resp, 'content', None)
                LOG.exception(
                    "Error getting users for cloud %r (%r): %r (%r)",
                    cid, clouds[cid], exc, resp)
            else:
                # actual_cids.append(cid)
                actual_sources.append(cloud_source)

        if not uid_to_user:
            LOG.error("No users to import from IAM")
            return

        users = uid_to_user.values()

        existing_lst: list[db_base.Subject] = []
        qs_base = (
            dbsess.query(Subject)
            .filter_by(
                kind='user',
                # active=True,
            ))
        # same source
        # XXXXXX: only processes one of user's clouds. Need a better solution.
        existing_lst += list(qs_base.filter(Subject.source.in_(actual_sources)))
        # same name (force-update them)
        existing_lst += list(qs_base.filter(Subject.name.in_([user['name'] for user in users])))

        existing = {item.name: item for item in existing_lst}
        target = set(user['name'] for user in users)
        obsolete = list(item for item in existing.values() if item.name not in target)

        aa = (
            dbsess.query(Subject)
            .filter_by(kind='group', name='system_group:all_active_users')
            .one())

        memberships_to_upsert = []
        for user in users:
            subject = existing.get(user['name'])
            if subject:
                for key, val in user.items():
                    setattr(subject, key, val)
                dbsess.add(subject)
            else:
                nc = NodeConfig(node_identifier=str(make_uuid()))
                dbsess.add(nc)
                subject = Subject(node_config=nc, **user)
                dbsess.add(subject)

            memberships_to_upsert.append(dict(group=aa, member=subject, defaults=dict(source=subject.source)))

        # run the inserts, get the `id`s of the new subjects (for the membership upserts).
        dbsess.flush()
        for membership_info in memberships_to_upsert:
            assert membership_info.get('member') and membership_info['member'].id
            # dbsess.add(membership(group=aa, member=subject, source=subject.source))
            get_or_create_sasess(dbsess, membership, **membership_info)

        if obsolete:
            LOG.info("Obsolete users: %r", len(obsolete))
            unmemberships = (
                dbsess.query(membership)
                .filter_by(group=aa)
                .filter(membership.member_id.in_([item.id for item in obsolete])))
            unmemberships.delete(synchronize_session=False)
            for user in obsolete:
                # "Incompatible types in assignment (expression has type "bool", variable has type "Column")"
                user.active = False  # type: ignore  # TODO: fix
                dbsess.add(user)

        dbsess.commit()

        if run_subjects_manager:
            LOG.info("Running subjects manager")
            from .subjects_manager import SubjectsManager
            smgr = SubjectsManager()
            smgr.db_engine = dbsess  # type: ignore  # TODO: fix  # `dbsess.bind`?
            smgr.main()
            dbsess.commit()

    def run_sync_import(self, **kwargs):
        db_session = self.db_get_session()
        self.db_session = db_session
        return self._run_sync_import_i(dbsess=db_session, **kwargs)


def main():
    from .dbg import init_logging
    init_logging(app_name='dlscloudmanager')
    mgr = CloudManagerSync()
    mgr.run_sync_import()


if __name__ == '__main__':
    main()

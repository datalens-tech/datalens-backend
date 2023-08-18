from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence

from bi_utils.sanitize import param_bool

from yadls import db_optimizations
from yadls.cloud_manager import CloudManagerAPI, CloudManagerCFG
from yadls.exceptions import UserError
from yadls.httpjrpc.base_app import (
    API_PREFIX, app, common_common_wrap, get_tenant_info, get_userid, settings,
)
from yadls.httpjrpc.qutils import simple_db_read, get_request_id
from yadls.subjects_manager import SubjectsManager

if TYPE_CHECKING:
    from contextlib import AsyncExitStack

    from yadls.manager_aiopg import DLSPG


@app.route('{}/nodes/all/<node_identifier>'.format(API_PREFIX), methods=['GET'])
@common_common_wrap(schema_path='/nodes/all/{node_identifier}')
async def node_info(node_identifier, mgr, **kwargs):
    node = await mgr._get_node_async(
        node_identifier, fetch_permissions=False, assume_entry=False)
    result = node._data
    meta = result.pop('meta')
    result = {**meta, **result}
    return result


@app.route('{}/nodes/all/<node_identifier>/access/<action>'.format(API_PREFIX), methods=['GET'])
@common_common_wrap(schema_path='/nodes/all/{node_identifier}/access/{action}')
async def check_access(node_identifier, action, mgr, request, **kwargs):
    call_kwargs = {}  # type: ignore  # TODO: fix

    optimization = request.args.get('_optimization')
    if optimization is None:
        pass
    elif optimization == 'single_sql':
        call_kwargs.update(optimized=True, _parallel=False, _single_sql=True)
    elif optimization == 'parallel':
        call_kwargs.update(optimized=True, _parallel=True, _single_sql=False)
    elif optimization == 'none':
        call_kwargs.update(optimized=False, _parallel=False, _single_sql='false')
    else:
        Exception("Unknown value of `_optimization`")

    try:
        extra_actions = request.args.getlist('extra_action')
    except KeyError:
        extra_actions = None

    return await mgr.check_permission_ext_async(
        user=request.args['user'],
        node=node_identifier,
        action=action,
        extra_actions=extra_actions,
        verbose=request.args.get('verbose'), **call_kwargs)


@app.route('{}/batch/render_subjects/'.format(API_PREFIX), methods=['POST'])
@common_common_wrap(schema_path='/batch/render_subjects/')
async def batch_render_subjects(mgr, request, data, handler_cmstack: AsyncExitStack, **kwargs):
    require = param_bool(request.args.get('require', 'true'))

    iam_token = request.headers.get('X-YaCloud-SubjectToken')

    subjects = data['subjects']

    details: dict[str, Any] = {}
    if settings.USE_CLOUD_SUGGEST:
        pfx = 'user:'
        user_ids = [name.removeprefix(pfx) for name in subjects if name.startswith(pfx)]
        other_ids = [name for name in subjects if not name.startswith(pfx)]
        cloud_manager = CloudManagerAPI(request_id=get_request_id(request))
        await handler_cmstack.enter_async_context(cloud_manager)
        subject_infos = await cloud_manager.subject_ids_to_infos(user_ids, iam_token=iam_token)
        subject_datas = [cloud_manager.subject_info_to_dls_data(subj) for subj in subject_infos.values()]
        results = {subj['name']: subj for subj in subject_datas}
        details.update(source='iam')
        if other_ids:
            db_results = await mgr.subject_names_to_infos(other_ids)
            details.update(source='iam,db')
            results.update(db_results)
    else:
        results = await mgr.subject_names_to_infos(subjects)
        details.update(source='db')

    # TODO: tenant filter (common)
    if require:
        missing = [item for item in subjects if item not in results]
        if missing:
            raise UserError(
                dict(
                    error='missing_subjects',
                    message="Some of the requested subjects are missing",
                    missing=missing),
                status_code=404)

    return dict(results=results, __details=details)


def _normalize_subject_login(login: str, suffix: str = '@' + CloudManagerAPI.DEFAULT_DOMAIN) -> str:
    login = login.strip()
    login = login.lower()
    login = login.removesuffix(suffix)
    return login


async def _subject_logins_to_info_by_db(
        subject_logins: Sequence[str], mgr: DLSPG, yacloud_folder_id: Optional[str] = None,
) -> tuple[dict[str, dict], dict[str, Any]]:
    words = ['__rlsid__:{}'.format(item) for item in subject_logins]
    wordsets = [words]
    if yacloud_folder_id and yacloud_folder_id != '-':
        realm_tag = f'__yacloudfolder__{yacloud_folder_id}'
        wordsets.append([realm_tag])

    smgr = SubjectsManager
    Subject = smgr.Subject.__table__
    stmt = smgr.search_statement_simple(
        wordsets,
        columns=[Subject.c.name],
    )
    debug_sql = str(stmt.compile())
    results = await simple_db_read(mgr, stmt)

    subject_names = [item.name for item in results]
    results_pre = await mgr.subject_names_to_infos(subject_names)
    result = {
        _normalize_subject_login(item.get('__rlsid') or ''): item
        for item in results_pre.values()}
    details = dict(source='db', sql=debug_sql)
    return result, details


async def _subject_logins_to_info_by_iam(
        subject_logins: Sequence[str], cloud_manager: CloudManagerAPI,
        tenant_info: dict, iam_token: Optional[str],
) -> tuple[dict[str, dict], dict[str, Any]]:
    subject_infos, details = await cloud_manager.subject_logins_to_infos(
        subject_logins, tenant_info=tenant_info, iam_token=iam_token)
    rendered_infos = {
        login: cloud_manager.subject_info_to_dls_data(subj)
        for login, subj in subject_infos.items()}
    details = dict(details, source='iam')
    return rendered_infos, details


@app.route('{}/batch/render_subjects_by_login/'.format(API_PREFIX), methods=['POST'])
@common_common_wrap(schema_path='/batch/render_subjects_by_login/')
async def batch_render_subjects_by_login(
        mgr: DLSPG, request, data, handler_cmstack: AsyncExitStack, **kwargs: Any,
) -> dict[str, Any]:
    require = param_bool(request.args.get('require', 'true'))

    iam_token = request.headers.get('X-YaCloud-SubjectToken')
    tenant_info = get_tenant_info(request, require=settings.REQUIRE_TENANT_FILTER)

    requested_subjects = data['subjects']
    subject_logins = list(set(_normalize_subject_login(subject) for subject in requested_subjects))

    details: dict[str, Any] = dict(tenant_info=tenant_info)

    if settings.USE_CLOUD_SUGGEST:
        cloud_manager = CloudManagerAPI(request_id=get_request_id(request))
        await handler_cmstack.enter_async_context(cloud_manager)
        results_pre, extra_details = await _subject_logins_to_info_by_iam(
            subject_logins=subject_logins,
            cloud_manager=cloud_manager,
            tenant_info=tenant_info,
            iam_token=iam_token,
        )
    else:
        results_pre, extra_details = await _subject_logins_to_info_by_db(
            subject_logins=subject_logins,
            mgr=mgr,
            yacloud_folder_id=tenant_info.get('folder_id'),
        )
    details.update(extra_details)

    results_pre = {
        _normalize_subject_login(subject_name): subject
        for subject_name, subject in results_pre.items()}
    results = {
        subject_name: results_pre.get(_normalize_subject_login(subject_name))
        for subject_name in requested_subjects}

    if require:
        missing = sorted(key for key, value in results.items() if value is None)
        if missing:
            raise UserError(
                dict(
                    error='missing_subjects',
                    message="Some of the requested subjects are missing",
                    missing=missing),
                status_code=404)

    return dict(results=results, __details=details)


@app.route('{}/batch_accesses'.format(API_PREFIX), methods=['POST'])
@common_common_wrap(schema_path='/batch_accesses')
async def batch_check_access_leg(mgr, request, data, **kwargs):
    # Deprecating
    try:
        extra_actions = request.args.getlist('extra_action')
    except KeyError:
        extra_actions = None

    return await mgr.check_permission_multi_async(
        user=get_userid(request, uid=data['user']),
        nodes=data['nodes'],
        action=data['action'],
        extra_actions=extra_actions,
        verbose=data.get('verbose'))


@app.route('{}/batch/accesses/'.format(API_PREFIX), methods=['POST'])
@common_common_wrap(schema_path='/batch/accesses/')
async def batch_check_access(mgr, request, data, **kwargs):

    try:
        extra_actions = request.args.getlist('extra_action')
    except KeyError:
        extra_actions = None

    results = await mgr.check_permission_multi_async(
        user=get_userid(request, uid=data['user']),
        nodes=data['nodes'],
        action=data['action'],
        extra_actions=extra_actions,
        verbose=data.get('verbose'))
    return dict(results=results)


@app.route('{}/cloud/'.format(API_PREFIX), methods=['GET'])
@common_common_wrap(schema_path='/cloud/')
async def cloud_cfg_get(mgr, **kwargs):
    cloud_manager = CloudManagerCFG(mgr)
    async with cloud_manager:
        return await cloud_manager.get_clouds_info()


@app.route('{}/cloud/<cloud_id>'.format(API_PREFIX), methods=['PUT'])
@common_common_wrap(schema_path='/cloud/{cloud_id}')
async def cloud_cfg_put(cloud_id, mgr, data=None, **kwargs):
    cloud_manager = CloudManagerCFG(mgr)
    async with cloud_manager:
        return await cloud_manager.add_cloud(cloud_id, meta=data)


@app.route('{}/cloud/<cloud_id>'.format(API_PREFIX), methods=['DELETE'])
@common_common_wrap(schema_path='/cloud/{cloud_id}')
async def cloud_cfg_delete(cloud_id, mgr, data=None, **kwargs):
    cloud_manager = CloudManagerCFG(mgr)
    async with cloud_manager:
        return await cloud_manager.remove_cloud(cloud_id, meta=data)


@app.route('{}/nodes/entries/<node_identifier>'.format(API_PREFIX), methods=['PUT'])
@common_common_wrap(schema_path='/nodes/entries/{node_identifier}', writing=True, tx=True)
async def add_entry(node_identifier, mgr, data, **kwargs):
    # data: {identifier, initialOwner, initialParent, initialPermissionsMode, scope}
    return await mgr.add_entry_async(data=dict(data, identifier=node_identifier))


@app.route('{}/nodes/subjects/<subject_identifier>/groups/'.format(API_PREFIX), methods=['GET'])
@common_common_wrap(schema_path='/nodes/subjects/{subject_identifier}/groups/')
async def subject_groups(subject_identifier, mgr, request, optimized=None, **kwargs):
    if optimized is None:
        optimized = request.args.get('_optimized', '1')
    if optimized:
        results = await simple_db_read(
            mgr,
            db_optimizations.CHECK_PERMS_SQL_SUBJECT_GROUPS_WITH_CACHE,
            subject=subject_identifier,
        )
        results = [item.data_ for item in results if item.type_ == 'subject_group']
    else:
        subject = await mgr._get_subject_node_async(
            subject_identifier, autocreate=False, fetch_effective_groups=True)
        # TODO: support getting more columns
        results = subject.get_effective_groups()
        results = list(results)
        # Unwrap into data:
        # TODO: some better abstraction for this, like `item.data_`
        results = [item._wrapped for item in results]

    if not request.args.get('_all'):
        results = [item for item in results if item['name'].startswith('system_group:')]

    return dict(results=results)

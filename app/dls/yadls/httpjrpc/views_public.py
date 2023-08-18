from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from yadls.cloud_manager import CloudManagerAPI
from yadls.subjects_manager import SubjectsManager

from yadls.httpjrpc.qutils import simple_db_read, get_request_id
from yadls.httpjrpc.base_app import (
    app,
    common_wrap,
    common_common_wrap,
    get_userid,
    API_PREFIX,
    settings,
    get_tenant_info,
)

if TYPE_CHECKING:
    from contextlib import AsyncExitStack

    from yadls.manager_aiopg import DLSPG


@app.route('{}/nodes/all/<node_identifier>/permissions'.format(API_PREFIX), methods=['GET'])
@common_common_wrap(schema_path='/nodes/all/{node_identifier}/permissions')
async def node_permissions_get(node_identifier, mgr, request, **kwargs):
    return await mgr.get_node_permissions_async(
        node=node_identifier,
        requester=get_userid(request),
        verbose=request.args.get('verbose'),
    )


@app.route('{}/nodes/all/<node_identifier>/permissions'.format(API_PREFIX), methods=['PATCH'])
@common_common_wrap(schema_path='/nodes/all/{node_identifier}/permissions', writing=True, tx=True)
async def node_permissions_patch(node_identifier, mgr, request, data, **kwargs):
    return await mgr.modify_node_permissions_async(
        node=node_identifier,
        requester=get_userid(request),
        default_comment=data.get('default_comment'),
        diff=data['diff'],
    )


@app.route('{}/nodes/all/<node_identifier>/permissions'.format(API_PREFIX), methods=['PUT'])
@common_common_wrap(schema_path='/nodes/all/{node_identifier}/permissions', writing=True, tx=True)
async def node_permissions_put(node_identifier, mgr, request, data, **kwargs):
    return await mgr.set_node_permissions_async(
        node=node_identifier,
        requester=get_userid(request),
        default_comment=data['default_comment'],
        permissions=data['permissions'],
    )


@app.route('{}/nodes/all/<node_identifier>/permissions/subjects/<subject_identifier>'.format(API_PREFIX), methods=['GET'])
@common_common_wrap(schema_path='/nodes/all/{node_identifier}/permissions/subjects/{subject_identifier}', writing=False, tx=False)
async def grant_info(node_identifier, subject_identifier, mgr, request, **kwargs):
    return await mgr.get_grants_info_async(
        node=node_identifier,
        subject=subject_identifier,
        requester=get_userid(request),
        verbose=request.args.get('verbose'),
    )


# TODO: add length and wordcount limit to the search text.
@app.route('{}/suggest/subjects/'.format(API_PREFIX), methods=['GET'])
@common_wrap(schema_path='/suggest/subjects/')
async def suggest_subjects(mgr: DLSPG, request, handler_cmstack: AsyncExitStack, **kwargs):
    search_text = request.args['search_text']
    limit = int(request.args.get('limit') or 5)

    tenant_info = get_tenant_info(request, require=settings.USE_CLOUD_SUGGEST)
    iam_token = request.headers.get('X-YaCloud-SubjectToken')

    cloud_manager = CloudManagerAPI(request_id=get_request_id(request))
    await handler_cmstack.enter_async_context(cloud_manager)

    language = request.headers.get('X-User-Language') or 'ru'
    # Required but not currently used: `get_userid(request)`

    sub_kwargs = dict(
        mgr=mgr, search_text=search_text, limit=limit, language=language,
    )

    extra_words: Sequence[tuple[str, ...]]
    if settings.USE_CLOUD_SUGGEST:
        # Only ask the db for subjects like 'All'
        extra_words = [
            ('__source__system',),
        ]
        sub_kwargs.update(extra_words=extra_words)
    else:
        sub_kwargs.update(with_all=False)

    results, details = await suggest_subjects_db(**sub_kwargs)

    remaining_limit = limit - len(results)
    if settings.USE_CLOUD_SUGGEST and remaining_limit > 0:
        details.update(source='db,iam' if results else 'iam')
        subject_infos = await cloud_manager.suggest_subjects(
            search_text=search_text,
            limit=remaining_limit,
            tenant_info=tenant_info,
            iam_token=iam_token,
        )
        subject_datas = [cloud_manager.subject_info_to_dls_data(subj) for subj in subject_infos]
        results = list(results) + list(subject_datas)

    return dict(results=results, __details=details)


async def suggest_subjects_db(mgr, search_text, limit, language, **kwargs) -> tuple[Sequence[dict[str, Any]], dict[str, Any]]:
    smgr = SubjectsManager

    stmt = smgr.search_statement(search_text, **kwargs)
    stmt = stmt.limit(limit)

    results_subjects = await simple_db_read(mgr, stmt)
    results_datas = [
        dict(
            smgr.subject_to_info(subject, language=language),
            _last_word=getattr(subject, 'last_word', None),
        )
        for subject in results_subjects
    ]
    details = dict(source='db', sql=str(stmt.compile()))
    return results_datas, details

from __future__ import annotations

from typing import Any

from bi_utils.aio import alist

from .utils import split_permissions, flatten_permissions, map_permissions, serialize_ts
from .exceptions import NotFound
from .manager_aiopg_base import ensure_db_reading, ensure_db_tx
from .manager_aiopg_api_common import DLSPGAPICommon


class DLSPGPublic(DLSPGAPICommon):
    """ User-facing minimally-wrapped API support """

    @ensure_db_reading
    async def get_node_permissions_async(self, node, requester, verbose=False):
        result = {}
        node = await self._get_node_async(node, fetch_permissions=False)
        node = await self._prefetch_node_permissions_async(node, active=None)

        def perm_to_subjects_names(perm):
            return (
                perm.name,
                perm.grant_meta.get('requester'),
                perm.grant_meta.get('approver'),
                # perm.meta.get('remover'),
            )

        all_perms = node.get_permissions_ext()
        all_subject_names = list(set(
            subject_name
            for perm in flatten_permissions(all_perms)
            for subject_name in perm_to_subjects_names(perm)
            if subject_name))
        all_subject_objs = list(flatten_permissions(all_perms))
        subject_to_info = await self.subject_names_to_infos(
            all_subject_names, subject_objs=all_subject_objs)

        def dump_perm(perm, **kwargs):
            item = dict(
                # Enforced identifier.
                name=perm.name,
                # Displayable info.
                subject=subject_to_info.get(perm.name, {}),
                # Grant-relevant metadata.
                kind=perm.kind,
                description=perm.grant_meta.get('description'),
                extras=perm.grant_meta.get('extras'),
                requester=subject_to_info.get(perm.grant_meta.get('requester')),
                approver=subject_to_info.get(perm.grant_meta.get('approver')),
                # _dbg__subject_meta=perm.meta, _dbg__grant_meta=perm.grant_meta,
            )
            return item

        def dump_perms(perms):
            result = map_permissions(perms, dump_perm)
            result = {perm_kind: sorted(subjects, key=lambda subject: subject.get('name'))
                      for perm_kind, subjects in result.items()}
            return result

        perms = all_perms
        perms, inactive_perms = split_permissions(perms, lambda subj: subj.grant_active)
        pending_perms, inactive_perms = split_permissions(
            inactive_perms, lambda subj: subj.grant_state == 'pending')

        result['permissions'] = dump_perms(perms)
        result['pendingPermissions'] = dump_perms(pending_perms)
        if verbose:
            result['_editable_info'] = await self.check_permission_ext_async(
                user=requester, node=node, action='set_permissions',
                verbose=True, optimized=False)
        result['editable'] = await self.modify_permissions_allowed(user=requester, node=node)
        return result

    def _fallback_subject_to_data(self, subject_name: str) -> dict[str, Any]:
        pieces = subject_name.split(':', 1)
        if len(pieces) == 2:
            subj_type, subj_name = pieces
            return dict(type=subj_type, title=subj_name)
        return dict(title=subject_name)

    async def subject_names_to_infos(self, subject_names, subject_objs=None) -> dict[str, dict[str, Any]]:
        smgr = self.subjects_manager_cls
        Subject = self.Subject

        result = {}
        name_to_subject = {}
        unknowns = subject_names

        if subject_objs is not None:
            name_to_subject.update({subj.name: subj for subj in subject_objs})
            unknowns = [name for name in unknowns if name not in name_to_subject]

        if unknowns:
            stmt = Subject.select_(Subject.name.in_(unknowns))
            rows = await self.db_conn.execute(stmt)
            subjs = await alist(rows)
            name_to_subject.update({subj.name: subj for subj in subjs})

        for name in subject_names:
            subj = name_to_subject.get(name)
            result[name] = (
                smgr.subject_to_info(subj)
                if subj is not None
                else self._fallback_subject_to_data(name))

        return result

    async def set_node_permissions_async(self, permissions, **kwargs):
        return await self.modify_node_permissions_async(
            diff=dict(removed={}, added=permissions),
            clear_all=True,
            **kwargs)

    @ensure_db_reading
    async def get_grants_info_async(self, node, subject, requester=None, verbose=False, active=None):
        node = await self._get_node_async(node, fetch_permissions=False)
        all_subject_objs = None

        # TODO?: replace with 'get single grant'?
        # However, the current form is supposed to be performant enough, and it
        # also makes it possible to avoid additional db poke for the subject
        # infos (using `all_subject_objs`).
        node = await self._prefetch_node_permissions_async(node, active=active)  # , subject_name=subject
        grants = flatten_permissions(node.get_permissions_ext())
        all_subject_objs = flatten_permissions(node.get_permissions())
        relevant_grants = list(item for item in grants if item.name == subject)
        if not relevant_grants:
            raise NotFound('Specified grant was not found')

        Log = self.Log
        stmt = Log.select_(Log.grant_guid.in_(grant.grant_guid for grant in relevant_grants))
        stmt = stmt.order_by(Log.ctime.desc())
        logs = await self.db_conn.execute(stmt)
        logs = await alist(logs)

        def log_to_subject_names(item):
            # yield item.request_user.name
            meta = item.meta
            yield meta.get('request_user_name')
            yield (meta.get('grant_data') or {}).get('subject_name')
            yield (meta.get('grant_data_prev') or {}).get('subject_name')

        subjects_names = set(
            name
            for item in logs
            for name in log_to_subject_names(item)
            if name)
        subjects = await self.subject_names_to_infos(subjects_names, subject_objs=all_subject_objs)

        def serialize_grant_data(grant_data):
            if not grant_data:
                return {}
            result = dict(
                state=grant_data.get('state'),
                active=grant_data.get('active'),
                grantType=grant_data.get('perm_kind'),
                description=grant_data.get('description'),
            )
            subject_name = grant_data.get('subject_name')
            if subject_name:
                result['subject'] = subjects.get(subject_name)
            return result

        def serialize_history_item(item):
            grant_data = serialize_grant_data(item.meta.get('grant_data'))
            grant_data_prev = serialize_grant_data(item.meta.get('grant_data_prev'))

            item_res = dict(
                timestamp=serialize_ts(item.mtime),
                action=item.meta.get('action'),
                author=subjects.get(item.meta.get('request_user_name')),
                comment=item.meta.get('comment'),
                extras=item.meta.get('extras') or {},

                grant=grant_data,
                previous={
                    key: val
                    for key, val in grant_data_prev.items()
                    # should be filtered before storage already, too.
                    if val != grant_data.get(key)},
                situation=item.meta.get('situation'),

                # Deprecating in favour of `previous`
                modified={
                    key: [val, grant_data.get(key)]
                    for key, val in grant_data_prev.items()
                    if val != grant_data.get(key)},
            )
            if verbose:
                item_res.update(
                    _debug=dict(
                    ),
                )
            return item_res

        history = list(serialize_history_item(item) for item in logs)
        result = dict(
            history=history,
        )
        return result

    @ensure_db_tx
    async def modify_node_permissions_async(
            self, node, requester, *args,
            return_get=True, **kwargs):
        result = await self._modify_node_permissions_async(node, requester, *args, **kwargs)
        if return_get:
            # Note: single-transaction modify+get.
            return await self.get_node_permissions_async(requester=requester, node=node)
        return result

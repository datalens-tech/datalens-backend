from __future__ import annotations

from typing import Any

from .utils import (
    datetime_now,
    map_permissions, flatten_permissions,
)
from .db_utils import bulk_upsert_statement
from .exceptions import UserError
from .node_classes import SubjectWrap
from .modify_permissions import ModifyPermissionsManager
from .manager_aiopg_base import ensure_db_tx
from .manager_aiopg_perm_check import DLSPGPermCheck


class DLSPGAPICommon(DLSPGPermCheck):
    """ Facilities used in both private and public API parts  """

    modify_perms_manager_cls = ModifyPermissionsManager

    async def modify_permissions_allowed(self, node, user):
        return await self.check_permission_async(
            user=user, node=node, action='set_permissions',
            optimized=False)

    @staticmethod
    def diff_item_to_subjects(diff_item):
        yield diff_item['subject']
        new_data = diff_item.get('new') or {}
        new_subject = new_data.get('subject')
        if new_subject:
            yield new_subject

    @ensure_db_tx
    async def _modify_node_permissions_async(
            self, node, requester, diff, autocreate_subjects=False,
            clear_all=False, default_comment=None, action=None,
            force_editable=None, extras=None, subject_objs=None, with_all=True,
    ) -> dict[str, Any]:
        """
        ...

        Used in:
        Public API;
        Private API on node create;
        """
        node_spec = node
        user_spec = requester
        user = await self._get_subject_node_async(user_spec, expected_kind='user')
        # NOTE: for_update, i.e. locking by the node.
        # Only need the node config (with denormalized values on it).
        node = await self._get_node_async(node_spec, fetch_permissions=False, for_update=True)
        # # Full data:
        # node = await self._get_node_async(node_spec, fetch_permissions=False)
        node = await self._prefetch_node_permissions_async(node, active=None)

        # Determine whether the requests are to be applied.
        if force_editable is not None:
            editable = force_editable
        else:
            editable = await self.modify_permissions_allowed(user=user, node=node)

        # Can bail early in here:
        if clear_all and not editable:
            raise UserError(dict(message='Not allowed to clear grants here'), status_code=403)

        # Reminder: diff is `{diff_act: {perm_kind: [{"subject": name, "comment": ...}, ...], ...}, ...}`
        all_subjects = {
            subject_name
            for perms in diff.values()
            for diff_item in flatten_permissions(perms)
            for subject_name in self.diff_item_to_subjects(diff_item)}
        all_subject_objs = await self._get_subjects_nodes_async(
            all_subjects,
            autocreate=autocreate_subjects,
            subject_objs=subject_objs,
        )
        name_to_subject = {subject.name: subject for subject in all_subject_objs}
        id_to_subject = {subject.id: subject for subject in all_subject_objs}

        perms_orig = node.get_permissions_ext()
        # diff_orig = diff
        # Turn each diff item into `SubjectWrap` with `.info_['comment']` and
        # `.info_['perm_kind']` and such populated.
        diff = {
            diff_key: map_permissions(
                perms,
                lambda diff_subject, perm_kind, **kwargs: SubjectWrap(
                    name_to_subject[diff_subject['subject']],
                    info=dict(
                        diff_subject,
                        comment=diff_subject.get('comment'),  # ensure it is set.
                        original_spec=diff_subject,
                        perm_kind=perm_kind)))
            for diff_key, perms in diff.items()}

        Grant = self.Grant

        scopes_info = await self._get_scopes_info_async()
        scope_info = self._get_scope_info(node.scope, scopes_info=scopes_info)
        perm_kind_sizes = scope_info['perm_kind_sizes']
        perm_kind_titles = scope_info['perm_kind_titles']

        manager = self.modify_perms_manager_cls(
            node=node, requester=user, diff=diff, perms_orig=perms_orig, editable=editable,
            perm_kind_sizes=perm_kind_sizes, perm_kind_titles=perm_kind_titles,
            clear_all=clear_all, default_comment=default_comment, action=action,
            id_to_subject=id_to_subject, name_to_subject=name_to_subject,
            context=self.context, extras_for_log=extras, extras_for_new_grants=extras,
        )
        try:
            manager.handle()
        except manager.NotAllowed as exc:
            raise UserError(exc.args[0], status_code=403)
        except manager.NotConsistent as exc:
            raise UserError(exc.args[0], status_code=400)

        to_delete = manager.to_delete
        to_upsert_base = manager.to_upsert

        mtime = datetime_now()

        delete_result = None
        if to_delete:
            raise Exception("This is not simply going to work because of FK to Log")
            # # Use case: a grant had its grantType modified over a previously
            # # existing but deleted grant.
            # stmt = Grant.delete_(
            #     Grant.id.in_([item.grant_id for item in to_delete]))
            # await self.db_conn.execute(stmt)

        result = None
        if to_upsert_base:
            to_upsert = list(to_upsert_base.values()) if isinstance(to_upsert_base, dict) else list(to_upsert_base)
            for item in to_upsert:
                if (
                    not with_all
                    and item['subject'] in ('system_group:all_active_users', 'system_group:staff_statleg')
                    and item['state'] != 'deleted'
                ):
                    raise UserError(dict(message='Not allowed adding `All` group'), status_code=403)
                assert item.get('guid')
                for key in ('id', 'ctime', 'subject'):  # server-side generated and internal
                    item.pop(key, None)
                item['mtime'] = mtime
                item.setdefault('realm', self.realm)
                item.setdefault('state', 'default')
            # NOTE: `ctime` is genreated on the db-side.
            # (don't get surprised if, suddenly, `ctime > mtime`)
            stmt = bulk_upsert_statement(
                table=Grant, items=to_upsert,
                # key=('node_config_id', 'subject_id'),
                # # Upserting by the `guid`, validating the logic by the
                # # unique_together `node_config_id, subject_id`.
                key=('guid',),
                columns=(
                    # key
                    'guid',
                    # editables
                    'perm_kind',
                    'subject_id',
                    'meta',
                    # logiced
                    'active',
                    'state',
                    # metadata
                    'mtime',
                ),
            )
            await self.db_conn.execute(stmt)
            # result = await alist(result)  # returns no rows.

        # # Disabling dls_log recording to reduce DB load (BI-4041)
        # to_logs = manager.to_logs
        # if to_logs:
        #     for item in to_logs:
        #         item['mtime'] = mtime
        #         item.setdefault('realm', self.realm)
        #
        #     stmt = (
        #         sa_pg.insert(self.Log)
        #         .values(to_logs)
        #         # .on_conflict_to_nothing()
        #     )
        #     await self.db_conn.execute(stmt)

        return dict(to_upsert=to_upsert_base, to_delete=to_delete, result=result, delete_result=delete_result)

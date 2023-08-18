from __future__ import annotations

import time
from asyncio import gather

from bi_utils.aio import alist
from bi_utils.utils import DotDict

from .utils import groupby
from .exceptions import NotFound, MultipleObjectsReturned, UserError
from .node_classes import SubjectWrap
from .manager_aiopg_base import (
    PERF_SINGLE_SQL_PERM_CHECK,
    PERF_PARALLEL_PERM_CHECK,
    ensure_db_reading,
)
from .manager_aiopg_confs import DLSPGConfs
from .manager_aiopg_entities import DLSPGEntities


class DLSPGPermCheck(DLSPGEntities, DLSPGConfs):
    """
    Permission checking.
    Exposed over api;
    Invoked internally;
    """

    @ensure_db_reading
    async def check_permission_async(self, user, node, action, **kwargs):
        result = await self.check_permission_ext_async(
            user=user, node=node, action=action, verbose=False, **kwargs)
        return result['result']

    async def _perm_check_data_straight(self, user, node, scopes_info=None):
        user = await self._get_subject_node_async(user, expected_kind='user', fetch_effective_groups=True)
        node = await self._get_node_async(node, fetch_permissions=True)
        if scopes_info is None:
            scopes_info = await self._get_scopes_info_async()
        return user, node, scopes_info

    async def _perm_check_data_parallel(self, user, node, scopes_info=None):
        # NOTE: `clone()` essentially causes a new db connection to be used for each.
        _things = (
            self.clone()._get_subject_node_async(user, expected_kind='user', fetch_effective_groups=True),
            self.clone()._get_node_async(node, fetch_permissions=True),
        )
        if scopes_info is None:
            _things += (self.clone()._get_scopes_info_async(),)  # type: ignore  # TODO: fix
        result = await gather(*_things)
        if scopes_info is not None:
            result += (scopes_info,)
        return result

    async def _perm_check_data_single_sql(self, user, node, scopes_info=None):
        assert isinstance(node, str)
        assert isinstance(user, str)
        node_spec = node
        subject_spec = user

        from .db_optimizations import CHECK_PERMS_SQL
        # NOTE: not currently expecting it to hit the db.
        if scopes_info is None:
            scopes_info = await self._get_scopes_info_async()
        full_data = await self.db_conn.execute(
            CHECK_PERMS_SQL,
            dict(subject=user, node=node))
        full_data = await alist(full_data)
        full_data = groupby((row['type_'], row['data_']) for row in full_data)
        node_res = full_data.get('node')
        if not node_res:
            raise NotFound('The specified node was not found: {!r}'.format(node_spec))
        if len(node_res) > 1:
            raise MultipleObjectsReturned(dict(rows=node_res))

        node = self.node_cls(DotDict(node_res[0]))  # type: ignore  # TODO: fix
        perms = groupby(  # {perm_kind: [subject_obj, ...]}
            (row['perm_kind'], self.subject_grant_cls(row, info=dict(row=row)))
            for row in [DotDict(item) for item in full_data.get('node_perm', [])])
        node.set_permissions(perms)

        subject_res = full_data.get('subject')
        if not subject_res:
            raise NotFound('The specified subject was not found: {!r}'.format(subject_spec))
        if len(subject_res) > 1:
            raise MultipleObjectsReturned(dict(rows=subject_res))
        user = self.subject_cls(DotDict(subject_res[0]))  # type: ignore  # TODO: fix
        groups = list(
            SubjectWrap(DotDict(item))
            for item in full_data.get('subject_group', []))
        user.set_effective_groups(groups)
        return user, node, scopes_info

    @ensure_db_reading
    async def check_permission_ext_async(
            self, user, node, action, scopes_info=None,
            optimized=True, assume_prepared=False,
            _parallel=PERF_PARALLEL_PERM_CHECK,
            _single_sql=PERF_SINGLE_SQL_PERM_CHECK,
            extra_actions=None,
            **kwargs):
        if assume_prepared:
            # assert isinstance(node, self.node_cls)
            # assert isinstance(user, self.user_cls)
            # assert user.kind == 'user'
            # assert scopes_info is not None
            pass
        elif optimized and _single_sql:
            user, node, scopes_info = await self._perm_check_data_single_sql(
                user=user, node=node, scopes_info=scopes_info)
        elif optimized and _parallel:
            user, node, scopes_info = await self._perm_check_data_parallel(
                user=user, node=node, scopes_info=scopes_info)
        else:
            user, node, scopes_info = await self._perm_check_data_straight(
                user=user, node=node, scopes_info=scopes_info)

        scope = node.scope
        scope_info = self._get_scope_info(scope, scopes_info=scopes_info)

        actions_info = scope_info['actions']
        try:
            perm_kinds = actions_info[action]
        except KeyError:
            raise NotFound('Unknown action {!r} for scope {!r}'.format(action, scope))

        extra_perm_kindses = None
        if extra_actions:
            extra_perm_kindses = {
                action: actions_info[action]
                for action in extra_actions}

        return await self.check_permission_by_perm_kinds_async(
            user=user, node=node, perm_kinds=perm_kinds,
            extra_perm_kindses=extra_perm_kindses,
            **kwargs)

    @ensure_db_reading
    async def check_permission_by_perm_kinds_async(
            self, user, node, perm_kinds,
            verbose=False, extra_perm_kindses=None,
            realm_check=False, do_log=True,
            **kwargs):
        '''
        ...

        Same as `check_permission_ext` but works on a prepared `perm_kinds` list,
        not an `action`.
        '''
        if realm_check:
            self.logger.warning('Something set realm_check=True in check_permission_by_perm_kinds_async')
        user = await self._get_subject_node_async(user, expected_kind='user', fetch_effective_groups=True)
        node = await self._get_node_async(node, fetch_permissions=True)

        extra = None
        extra_meta = None

        if realm_check and self.realm and node.realm and self.realm != node.realm:
            is_allowed = False
            meta = dict(
                reason='realm',
                message="Wrong tenant on the node",
                requested=self.realm,
                existing=node.realm,
            )
        else:
            is_allowed, meta = self.check_permission_by_perm_kinds_base(
                user_obj=user, node_obj=node, perm_kinds=perm_kinds, **kwargs)
            if extra_perm_kindses:
                extra = {}
                extra_meta = {}
                for extra_key, extra_perm_kinds in extra_perm_kindses.items():
                    extra[extra_key], extra_meta[extra_key] = self.check_permission_by_perm_kinds_base(
                        user_obj=user, node_obj=node, perm_kinds=extra_perm_kinds, **kwargs)

        meta = dict(meta, user=user, node=node, perm_kinds=perm_kinds)
        result = dict(result=is_allowed)
        if do_log:
            self.logger.info(
                "Permission check (user=%r, node=%r, perm_kinds=%r) is_allowed: %r",
                user.name, node.identifier, perm_kinds, is_allowed,
                extra=dict(is_allowed=is_allowed, permissions_meta=meta))
        if extra_perm_kindses:
            result.update(extra=extra)  # type: ignore  # TODO: fix
        if verbose:
            result.update(meta=meta)  # type: ignore  # TODO: fix
            if extra_perm_kindses:
                result.update(extra_meta=extra_meta)  # type: ignore  # TODO: fix
        return result

    def check_permission_by_perm_kinds_base(  # pylint: disable=too-many-arguments,too-many-locals
            self, user_obj, node_obj, perm_kinds,
            with_superuser='auto',
            with_sudo='auto',
            **kwargs):

        if with_sudo == 'auto':
            with_sudo = self.context.get('sudo', False)  # type: ignore  # TODO: fix
        if with_superuser == 'auto':
            with_superuser = self.context.get('allow_superuser', False)  # type: ignore  # TODO: fix

        if with_sudo:
            user_groups = user_obj.get_effective_groups()
            assert isinstance(user_groups, set)
            # `user_groups = ensure_set(user_groups)`, but it's performance-important.
            superuser_group = self.get_superuser_group()
            is_superuser = superuser_group in user_groups
            if not is_superuser:
                raise UserError(dict(message='sudo: not a superuser'), status_code=403)
            with_superuser = True  # sudo => allow_superuser
            # Still fall through to the base logic, to handle `verbose` and other possible additions.

        return super().check_permission_by_perm_kinds_base(
            user_obj=user_obj, node_obj=node_obj, perm_kinds=perm_kinds,
            with_superuser=with_superuser,
            **kwargs)

    @ensure_db_reading
    async def check_permission_multi_async(self, user, nodes, action, **kwargs):
        nodes_spec = nodes
        user_spec = user
        user = await self._get_subject_node_async(user, expected_kind='user', fetch_effective_groups=True)
        nodes_objs = await self._get_multi_nodes_async(nodes, fetch_permissions=False)
        nodes_objs = await self._prefetch_multi_nodes_permissions_async(nodes_objs, detailed=False)

        # NOTE: not currently expecting it to hit the db.
        scopes_info = await self._get_scopes_info_async()

        t1 = time.time()
        result = await self._check_permission_multi_async_i(
            user=user, nodes_objs=nodes_objs, action=action,
            user_spec=user_spec, nodes_spec=nodes_spec,
            scopes_info=scopes_info,
            **kwargs)
        t2 = time.time()
        td = t2 - t1
        self.logger.info(
            "check_permission_multi_async: inner part took %.3fs / %d nodes (%.5fs/node)",
            td, len(nodes_objs), td / (len(nodes_objs) or -1))
        return result

    async def _check_permission_multi_async_i(
            self, user, nodes_objs, action,
            user_spec=None, nodes_spec=None, do_log=False,
            **kwargs):
        identifier_to_node = {node.identifier: node for node in nodes_objs}
        results = {}
        for node_identifier in nodes_spec:
            node = identifier_to_node.get(node_identifier)
            if node is None:
                result = dict(result=None, status='not found')
            else:
                # NOTE: should not actually do any i/o.
                node_res = await self.check_permission_ext_async(
                    user=user, node=node, action=action,
                    optimized=False, assume_prepared=True,
                    do_log=do_log,
                    **kwargs)
                result = dict(node_res, status=None)
            results[node_identifier] = result
        return results

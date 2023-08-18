from __future__ import annotations

import sqlalchemy.dialects.postgresql as sa_pg

from .utils import make_uuid, map_permissions, copy_permissions
from .db_utils import upsert_statement, db_insert_one
from .manager_aiopg_base import ensure_db_writing
from .manager_aiopg_api_common import DLSPGAPICommon


class DLSPGPrivate(DLSPGAPICommon):
    """ Facilities for supporting the private API pieces """

    @ensure_db_writing
    async def add_entry_async(self, *args, **kwargs):
        async with self.db.manage(writing=True, tx=True):
            return await self.add_entry_async_i(*args, **kwargs)

    @ensure_db_writing
    async def add_entry_async_i(self, data):
        Node = self.Node
        NodeConfig = self.NodeConfig

        node_upper_keys = ('identifier', 'scope')
        node_meta = {key: val for key, val in data.items() if key not in node_upper_keys}
        node_data = {
            **{key: data[key] for key in node_upper_keys},
            **{'meta': node_meta, 'realm': self.realm}}
        node_insert = upsert_statement(table=Node, values=node_data, key=('identifier',))
        node_id = await db_insert_one(self.db_conn, node_insert)

        cfg_data = dict(
            node_id=node_id,
            node_identifier=data['identifier'],
            scope=data['scope'],
            realm=self.realm,
        )
        cfg_insert = upsert_statement(table=NodeConfig, values=cfg_data, key=('node_identifier',))
        cfg_id = await db_insert_one(self.db_conn, cfg_insert)
        perms_res = await self.fill_entry_default_permissions(
            node_data=node_data, node_id=node_id, cfg_id=cfg_id)
        return dict(node_id=node_id, cfg_id=cfg_id, perms_res=perms_res)

    @ensure_db_writing
    async def fill_entry_default_permissions(self, node_data, node_id, cfg_id=None, comment='', add_extras=True):
        mode = node_data['meta']['initialPermissionsMode']
        owner_name = node_data['meta']['initialOwner']

        if mode == 'void':
            return dict(status='void')

        # Only for autocreate
        owner = await self._get_subject_node_async(
            owner_name, autocreate=True, expected_kind='user', fetch_effective_groups=False)
        assert owner

        if mode == 'parent_and_owner':
            parent_id = node_data['meta']['initialParent']
            parent = await self._get_node_async(node=parent_id, fetch_permissions=True, assume_entry=True)
            parent_permissions = parent.get_permissions()
            # TODO: `map_permissions(parent_permissions, lambda subject: subject.name)`
            permissions = map_permissions(parent_permissions, lambda subject, **kwargs: subject.name)
            acl_adm = permissions.setdefault('acl_adm', [])
            if owner_name not in acl_adm:
                acl_adm.append(owner_name)
        elif mode == 'owner_only':
            permissions = dict(acl_adm=[owner_name])
        elif mode == 'explicit':
            permissions = node_data['meta']['initialPermissions']
        else:
            raise Exception("Unknown initialPermissionsMode: {!r}".format(mode))

        def process_permission(perm, **kwargs):
            if isinstance(perm, dict):
                perm = perm.copy()
                perm.setdefault('comment', comment)
            else:
                perm = dict(subject=perm, comment=comment)
            return perm

        diff_permissions = map_permissions(permissions, process_permission)

        modify_res = await self._modify_node_permissions_async(
            # TODO/PERF: mockup the entire NodeWrap object
            node=node_data['identifier'],
            requester='system_user:root',  # internal work
            diff=dict(added=diff_permissions),
            # `extras_for_log_and_new_grants`
            extras=dict(initial_on_create=True) if add_extras else {},
            force_editable=True,  # internal work to account for `with_superuser=False`
            # TODO/PERF: pass the subjects info
        )
        return dict(modify_res=modify_res)

    @ensure_db_writing
    async def fill_entry_default_permissions_overly_straight(self, node_data, node_id, cfg_id=None):
        mode = node_data['meta']['initialPermissionsMode']
        owner = node_data['meta']['initialOwner']
        if mode == 'void':
            return []

        owner = await self._get_subject_node_async(
            owner, expected_kind='user', fetch_effective_groups=False)
        if mode == 'parent_and_owner':
            parent_id = node_data['meta']['initialParent']
            parent = await self._get_node_async(node=parent_id, fetch_permissions=True, assume_entry=True)
            permissions = copy_permissions(parent.get_permissions())
            # NOTE: if duplicated, it will be joined in the db.
            permissions.setdefault('acl_adm', []).append(owner)
        elif mode == 'owner_only':
            permissions = dict(acl_adm=[owner])

        Grant = self.Grant
        meta = dict(
            description='',
            extras=dict(initial_on_create=True),
        )
        objs = list(  # `Grant` datas
            dict(
                node_config_id=cfg_id,
                subject_id=subject.id,
                perm_kind=perm_kind,
                active=True,
                state='active',
                meta=meta,
                guid=make_uuid(),
            )
            for perm_kind, subjects in permissions.items()
            for subject in set(subjects))
        insert = sa_pg.insert(Grant).values(objs)
        insert = insert.on_conflict_do_nothing()  # NOTE.
        await self.db_conn.execute(insert)  # returns nothing
        return objs

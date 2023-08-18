# coding: utf8
"""
...
"""


from __future__ import annotations

import copy
import logging
from collections import defaultdict

from . import core
from .manager_base import DLSBase, EntryBase, GroupBase, NodeBase, SubjectBase, UserBase
from .utils import ensure_set, groupby


class InmemMixin(object):

    _default_data = {}  # type: ignore  # TODO: fix

    def __init__(self, **kwargs):
        try:
            data = kwargs.pop('data')
        except KeyError:
            data = copy.deepcopy(self._default_data)
        self.data = data

        super().__init__(**kwargs)  # type: ignore  # TODO: fix


class NodeInmem(InmemMixin, NodeBase):  # type: ignore  # TODO: fix

    _default_data = {"permissions": {}}  # type: ignore  # TODO: fix

    def get_permissions(self, perm_kinds=None, **kwargs):
        result = self.data['permissions']
        if perm_kinds:
            result = {perm_kind: result.get(perm_kind) for perm_kind in perm_kinds}
        return result

    def set_permissions(self, permissions, **kwargs):
        self.data['permissions'] = permissions


class EntryInmem(NodeInmem, EntryBase):  # type: ignore  # TODO: fix
    """ ... """


class SubjectInmem(NodeInmem, SubjectBase):  # type: ignore  # TODO: fix
    """ ... """


class UserInmem(SubjectInmem, UserBase):  # type: ignore  # TODO: fix
    """ ... """

    def get_effective_groups(self):
        # NOTE: the key might not be present on auto-created users.
        return self.data.get('effective_groups') or ()


class GroupInmem(SubjectInmem, GroupBase):  # type: ignore  # TODO: fix
    """ ... """

    _default_data = dict(subjects=[])  # type: ignore  # TODO: fix

    def get_subjects(self):
        return self.data['subjects']

    def set_subjects(self, subjects, **kwargs):
        self.data['subjects'] = subjects

    def add_subjects(self, subjects, **kwargs):
        self.data['subjects'] += subjects

    def remove_subjects(self, subjects, **kwargs):
        self.data['subjects'] = list(
            subject for subject in self.data['subjects']
            if subject not in subjects)


def _generate_user_effective_groups(
        groups, user_base_cls=UserBase, group_base_cls=GroupBase,
        raise_on_cycle=False):
    """
    Given a groups list, return a mapping from user to effective groups,
    alongside metadata.
    """
    group_to_subjects = {group: group.get_subjects() for group in groups}

    # Flatten the `group_to_subjects` into `group_to_users`
    group_to_users_memo = {}  # type: ignore  # TODO: fix
    stats = defaultdict(int)  # type: ignore  # TODO: fix

    def group_to_users(group, path=()):
        result = group_to_users_memo.get(group)
        if result is not None:
            stats['memoized'] += 1
            return result

        recurse = True
        if group in path:
            if raise_on_cycle:
                raise core.CycleFound(path + (group,))
            stats['cycle'] += 1
            recurse = False

        result = []
        subjects = group_to_subjects[group]
        new_path = set(path)
        new_path.add(group)
        for subject in subjects:
            if isinstance(subject, user_base_cls):
                result.append(subject)
            elif isinstance(subject, group_base_cls) and recurse:
                for subsubject in group_to_users(subject, path=new_path):
                    result.append(subsubject)

        if not recurse:
            return result

        stats['generated'] += 1
        group_to_users_memo[group] = result
        return result

    group_to_users_result = {group: group_to_users(group) for group in groups}
    result = groupby(
        (user, group)
        for group, users in group_to_users_result.items()
        for user in users)
    result = {user: set(groups) for user, groups in result.items()}
    return result, dict(stats=stats, group_to_users=group_to_users)


class DLSInmem(DLSBase):
    """
    A reference implementation of the DLS Manager class that uses only
    in-memory storage.
    """

    node_cls = NodeInmem  # type: ignore  # TODO: fix
    entry_cls = EntryInmem  # type: ignore  # TODO: fix
    subject_cls = SubjectInmem  # type: ignore  # TODO: fix
    user_cls = UserInmem
    group_cls = GroupInmem

    logger = logging.getLogger('DLSInmem')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.store = dict(  # type: ignore  # TODO: fix
            configurations={},
            nodes={},
            entries={},
            users={},
            groups={},
            path_to_node={},
            id_to_node={},
        )
        self._effective_groups_clean = True

    def _load_storage(self):
        """ ... """
        # ...

    _skip_save_storage = False

    def _save_storage(self, **kwargs):
        """ ... """
        if self._skip_save_storage:
            return
        # ...

    def _ensure_effective_groups(self):
        if self._effective_groups_clean:
            return
        self._generate_effective_groups()
        self._effective_groups_clean = True

    def _generate_effective_groups(self):
        self.logger.info("_generate_effective_groups()")
        store = self.store
        user_to_groups, meta = _generate_user_effective_groups(store['groups'].values())
        self.logger.debug("_generate_user_effective_groups meta: %r", meta)
        for user in store['users'].values():
            user.data['effective_groups'] = ensure_set(user_to_groups.get(user) or set())
        self._save_storage(context="_generate_effective_groups")

    # pylint: disable=arguments-differ
    def check_permission_by_perm_kinds(self, user, node, perm_kinds, verbose=False, **kwargs):
        self._ensure_effective_groups()
        results = super().check_permission_by_perm_kinds(
            user=user, node=node, perm_kinds=perm_kinds, verbose=verbose, **kwargs)
        self.logger.debug("check_permission_by_perm_kinds -> %r", results)
        return results

    def get_configuration(self, name):
        return self.store['configurations'][name]

    def set_configuration(self, name, data):
        self.store['configurations'][name] = data
        self._save_storage(context="set_configuration")

    def _get_node(self, node, require=True, **kwargs):  # pylint: disable=arguments-differ
        if isinstance(node, core.NodeBase):
            return node

        store = self.store
        node_orig = node
        result = None

        if isinstance(node, str):
            result = store['id_to_node'].get(node)

        if isinstance(node, list):
            node = tuple(node)

        if isinstance(node, tuple):
            if not all(isinstance(subval, str) for subval in node):
                raise ValueError("Invalid supposedly-path specification", node)
            result = store['path_to_node'].get(node)

        if result is None and require:
            raise ValueError("Unknown node", node_orig)
        return super()._get_node(result, require=require, **kwargs)

    def _get_subject_node(self, node, auto_create_user=True, **kwargs):
        result = super()._get_subject_node(node, require=False, **kwargs)
        if result is None:
            if (not auto_create_user or
                    not isinstance(node, (list, tuple)) or
                    node[0] not in (".users", ".system_users")):
                return super()._get_subject_node(node, require=True, **kwargs)
            path = tuple(node)
            result = self._add_user(path=path)
        return result

    def _node_to_stores(self, node, main_store_key):
        return (
            (main_store_key, node),
            ('path_to_node', node.path),
            ('id_to_node', node.id),
        )

    def _add_node(self, node, main_store_key):
        store = self.store
        parts = self._node_to_stores(node=node, main_store_key=main_store_key)
        for store_key, node_key in parts:
            existing_node = store[store_key].get(node_key)
            if existing_node:
                raise ValueError("Node conflict", dict(
                    store_key=store_key, node_key=node_key, node=node,
                    existing_node=existing_node))
        for store_key, node_key in parts:
            store[store_key][node_key] = node
        self._save_storage(context="_add_node")
        return node

    def _delete_node(self, node, main_store_key):
        store = self.store
        parts = self._node_to_stores(node=node, main_store_key=main_store_key)
        results = {}
        for store_key, node_key in parts:
            part_res = store[store_key].pop(node_key, None)
            results[store_key] = bool(part_res)
        self._save_storage(context="_delete_node")
        return results

    def add_entry_obj(self, entry, **kwargs):
        return self._add_node(entry, main_store_key='entries')

    # pylint: disable=redefined-builtin
    def delete_entries(self, entries, apply=False, **kwargs):
        resolved_entries = list(
            (entry_id, self._get_node(entry_id, require=False, expected_type=self.entry_cls))  # type: ignore  # TODO: fix
            for entry_id in entries)
        missing = list(entry_id for entry_id, entry in resolved_entries if not entry)
        if missing:
            raise Exception("Not all entries to be deleted are present", missing)
        if not apply:
            return []
        results = []
        for entry in entries:
            entry_res = self._delete_node(entry, 'entries')
            results.append(entry_res)
        return results

    def _add_user(self, **data):
        user = self._make_node(self.user_cls, data)
        return self._add_node(user, 'users')

    def _delete_user(self, user, **kwargs):
        user = self._get_node(user)
        return self._delete_node(user, 'users')

    def add_group_obj(self, group, **kwargs):
        group = self._add_node(group, 'groups')
        if group.get_subjects():
            self._effective_groups_clean = False
        return group

    def delete_group(self, group, **kwargs):
        group = self._get_node(group)
        if group.get_subjects():
            self._effective_groups_clean = False
        return self._delete_node(group, 'groups')

    def set_group_subjects(self, group, subjects, **kwargs):
        super().set_group_subjects(group=group, subjects=subjects, **kwargs)
        self._effective_groups_clean = False
        self._save_storage(context="set_group_subjects")

    def add_group_subjects(self, group, subjects, **kwargs):
        super().add_group_subjects(group=group, subjects=subjects, **kwargs)
        self._effective_groups_clean = False
        self._save_storage(context="add_group_subjects")

    def remove_group_subjects(self, group, subjects, **kwargs):
        super().remove_group_subjects(group=group, subjects=subjects, **kwargs)
        self._effective_groups_clean = False
        self._save_storage(context="remove_group_subjects")

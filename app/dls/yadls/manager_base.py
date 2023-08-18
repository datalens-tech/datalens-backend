"""
Non-backend-specific partial implementation of a permissions manager.
"""


from __future__ import annotations

from . import core
from .exceptions import NotFound
from .utils import ReprMixin, StateSuperMixin, ensure_set, make_uuid, prefixed_uuid


__all__ = (
    'NotFound',
    'GUIDedMixin',
    'PathMixin',
    'ParentMixin',
    'NodeBase',
    'EntryBase',
    'SubjectBase',
    'UserBase',
    'GroupBase',
    'DLSBase',
)


class GUIDedMixin:
    """
    A mixin to maintain auto-generated `self.id`.
    """

    def __init__(self, id=None, **kwargs):  # pylint: disable=redefined-builtin
        if id is None:
            id = make_uuid()
        self.id = id
        super().__init__(**kwargs)  # type: ignore  # TODO: fix

    def __hash__(self):
        return hash(self.id)


class PathMixin:
    """
    A mixin to maintain `self.path` tuple.
    """

    def __init__(self, **kwargs):
        self.path = tuple(kwargs.pop('path', ()))
        super().__init__(**kwargs)  # type: ignore  # TODO: fix


class ParentMixin(StateSuperMixin):
    """
    A mixin to maintain the object's manager, ignored in deepcopy/pickle.
    """

    def __init__(self, **kwargs):
        parent = kwargs.pop('_parent', None)
        self._parent = parent
        super().__init__(**kwargs)  # type: ignore  # TODO: fix

    def __getstate__(self):
        state = super().__getstate__()
        state.pop('_parent', None)
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self._parent = None


class NodeBase(GUIDedMixin, ReprMixin, PathMixin, ParentMixin, core.NodeBase):  # type: ignore  # TODO: fix
    """ A common mixin class for Node, providing optional defaults """

    _repr_keys = ('name', 'id', 'path')  # type: ignore  # TODO: fix

    def __init__(self, kind=None, name=None, meta=None, **kwargs):
        """
        An object tree node.

        :param id: an uuid4 of the object. If not set, a new one is generated.
        :param path: a tuple of strings denoting the location of the Node. Partially optional.
        """
        super().__init__(**kwargs)
        self.name = name
        self.meta = dict(meta or {})
        if kind is not None:
            if self.kind is None:
                self.kind = kind
            else:
                raise ValueError(
                    "Attempting to override `kind` of a special Node",
                    dict(self=self, kind=kind))
        # For non-entry nodes, `scope == kind` (for `special_scope` support)
        self.scope = self.kind

    def __hash__(self):
        # NOTE: exactly the same hash as the value itself.
        return hash(self.path or self.id)


class EntryBase(NodeBase, core.EntryBase):  # type: ignore  # TODO: fix
    """ ... """
    # kind = "entry"

    def __init__(self, *args, scope=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.scope = scope


class SubjectBase(NodeBase, core.SubjectBase):  # type: ignore  # TODO: fix
    """ ... """
    kind = "subject"  # type: ignore  # TODO: fix


class UserBase(SubjectBase, core.UserBase):  # type: ignore  # TODO: fix

    kind = "user"  # type: ignore  # TODO: fix

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.path = self.path or ('.users', self.name or self.id)


class GroupBase(SubjectBase, core.GroupBase):  # type: ignore  # TODO: fix

    kind = "usergroup"  # type: ignore  # TODO: fix

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.path = self.path or ('.groups', self.name or self.id)


class DLSBase(core.DLSBase):

    node_cls = NodeBase
    entry_cls = EntryBase
    subject_cls = SubjectBase
    user_cls = UserBase
    group_cls = GroupBase

    custom_scopes_enabled = False

    def _get_node(self, node, expected_type=None, expected_kind=None, require=True):
        """
        Overridable point for `node_identifier` -> `node_instance`.
        """
        if node is None:
            if not require:
                return None
            raise ValueError("Null node identifier")
        if not isinstance(node, self.node_cls):
            raise ValueError("Not a node instance", node)
        node = self._ensure_node_type(
            node, expected_type=expected_type, expected_kind=expected_kind)
        return node

    def _get_subject_node(self, node, auto_create_user=True, **kwargs):
        kwargs.setdefault('expected_type', self.subject_cls)
        return self._get_node(node, **kwargs)

    def _get_scope_info(self, scope, scopes_info=None):
        if scope in self.special_scopes_info:
            return self.special_scopes_info[scope]

        if self.custom_scopes_enabled:
            if scopes_info is None:
                scopes_info = self.get_configuration('scopes') or {}
            scope_conf = scopes_info.get(scope)
            if scope_conf is None:
                raise ValueError("Unknown scope", scope)
        else:
            scope_conf = self.default_scope_info
        return self._normalize_scope_info(scope_conf)

    def _normalize_scope_info(self, data, force=False):
        if not force and data.get('__normalized'):
            return data

        perm_kinds = data.get('perm_kinds') or []
        special_perm_kinds = list(item['kind'] for item in self.special_perm_kinds_info)
        perm_kinds = [
            perm_kind for perm_kind in perm_kinds
            if perm_kind not in special_perm_kinds]
        perm_kinds = special_perm_kinds + perm_kinds
        data['perm_kinds'] = perm_kinds

        actions = data.get('actions') or {}
        actions = {**self.default_scope_info['actions'], **actions}  # type: ignore  # TODO: fix
        data['actions'] = actions

        data['__normalized'] = True
        return data

    def check_permission_ext(self, user, node, action, **kwargs):
        node = self._get_node(node)
        scope = node.scope
        scope_info = self._get_scope_info(scope)
        actions_info = scope_info['actions']
        try:
            perm_kinds = actions_info[action]
        except KeyError:
            raise ValueError("Unknown action", action)
        return self.check_permission_by_perm_kinds(
            user=user, node=node, perm_kinds=perm_kinds, **kwargs)

    # pylint: disable=arguments-differ
    def check_permission_by_perm_kinds(
            self, user, node, perm_kinds, verbose=False,
            assert_check_result=None, **kwargs):
        """
        ...

        Same as `check_permission_ext` but works on a prepared `perm_kinds` list,
        not an `action`.

        :returns: `is_allowed, metadata`
        """
        user = self._get_subject_node(user, expected_kind='user')
        node = self._get_node(node)
        is_allowed, meta = self.check_permission_by_perm_kinds_base(
            user_obj=user, node_obj=node, perm_kinds=perm_kinds, **kwargs)
        meta = dict(meta, user=user, node=node, perm_kinds=perm_kinds)
        result = dict(result=is_allowed)
        if verbose:
            result.update(meta=meta)
        if assert_check_result is not None:
            assert is_allowed == assert_check_result
        return result

    def check_permission_by_perm_kinds_base(  # pylint: disable=too-many-arguments,too-many-locals
            self, user_obj, node_obj, perm_kinds,
            with_acl_deny=True,
            with_acl_adm=True,
            with_superuser=True,
            # NOTE:
            with_active_check=False,
            **kwargs):
        """
        Reference implementation.

        Same as `check_permission_by_perm_kinds` but works over implemented
        node objects.
        """
        meta = dict(
            user_obj=user_obj,
            node_obj=node_obj,
            perm_kinds=perm_kinds,
            kwargs=kwargs,
            reason=None,
            with_superuser=with_superuser,
        )
        perms = self.get_node_permissions(node_obj, perm_kinds=tuple(perm_kinds) + ('acl_deny',))
        meta['perms'] = perms

        user_groups = ensure_set(user_obj.get_effective_groups())
        meta['user_groups'] = list(user_groups)  # for debug

        def check_acl_ext(perm_kind):
            check_meta = dict(acl=None, groups=None)
            subjects = ensure_set(perms.get(perm_kind) or ())
            if user_obj in subjects:
                return True, dict(check_meta, acl='listed_directly')
            matching_groups = user_groups & subjects
            if matching_groups:
                return True, dict(
                    check_meta,
                    acl='listed_by_groups',
                    matching_groups=matching_groups,
                    matching_groups_lst=sorted(matching_groups),
                )
            return False, check_meta

        def check_acl(perm_kind):
            result, check_meta = check_acl_ext(perm_kind)
            meta.update(check_meta)  # NOTE: mutating the in-func debug data.
            return result

        if with_superuser:  # Highest priority (even before acl_deny).
            superuser_group = self.get_superuser_group()
            if superuser_group is not None and superuser_group in user_groups:
                return True, dict(meta, reason='superuser', superuser_group=superuser_group)

        if with_active_check:
            active_user_group = self.get_active_user_group()
            if (active_user_group is not None and
                    active_user_group not in user_groups):
                return False, dict(
                    meta, reason='not_an_active_user', active_user_group=active_user_group)
        else:  # otherwise assume the user is active whatever the database says
            active_user_group = self.get_active_user_group()
            if (active_user_group is not None and
                    active_user_group not in user_groups):
                user_groups = user_groups.copy()
                user_groups.add(active_user_group)

        if with_acl_deny and check_acl('acl_deny'):
            return False, dict(meta, reason='acl_deny')

        if with_acl_adm and check_acl('acl_adm'):
            return True, dict(meta, reason='acl_adm')

        for perm_kind in perm_kinds:
            if check_acl(perm_kind):
                return True, dict(meta, reason=perm_kind)

        return False, dict(meta, reason='not_in_any_list')

    SUPERUSER_GROUP_PATH = ('.system_groups', 'superuser')
    SUPERUSER_GROUP_UUID = prefixed_uuid('s_g_superuser')
    SUPERUSER_GROUP_NAME = 'system_group:superuser'

    def get_superuser_group(self):
        return self._get_node(
            self.SUPERUSER_GROUP_PATH, expected_type=self.group_cls, require=False)

    ACTIVE_USER_GROUP_PATH = ('.system_groups', 'active')
    ACTIVE_USER_GROUP_UUID = prefixed_uuid('s_g_active')
    ACTIVE_USER_GROUP_NAME = 'system_group:all_active_users'

    def get_active_user_group(self):
        return self._get_node(
            self.ACTIVE_USER_GROUP_PATH, expected_type=self.group_cls, require=False)

    @classmethod
    def get_system_groups_data(cls):
        return (
            dict(
                kind='group', source='system',
                name=cls.SUPERUSER_GROUP_NAME,
                uuid=cls.SUPERUSER_GROUP_UUID,
                search_weight=-1000,
                meta=dict(
                    name='superuser',
                    title=dict(ru='Администратор', en='Superuser'),
                )),
            dict(
                kind='group', source='system',
                name=cls.ACTIVE_USER_GROUP_NAME, uuid=cls.ACTIVE_USER_GROUP_UUID,
                search_weight=128,
                meta=dict(
                    name='all',
                    title=dict(ru='Все', en='All'),
                )),
        )

    def _make_node(self, cls, data):
        return cls(_parent=self, **data)

    def _ensure_node_type(self, node, expected_type=None, expected_kind=None):
        if expected_kind is not None and node.kind != expected_kind:
            raise ValueError("Wrong node kind", dict(
                got=node.kind, expected=expected_kind))
        if expected_type is not None and not isinstance(node, expected_type):
            raise ValueError("Wrong node type", dict(
                got=type(node), expected=expected_type))
        return node

    # def get_configuration(self, name):
    #     raise NotImplementedError

    # def set_configuration(self, name, data):
    #     raise NotImplementedError

    def add_entry(self, **data):
        entry = self._make_node(self.entry_cls, data)
        return self.add_entry_obj(entry) or entry

    def add_entry_obj(self, entry, **kwargs):
        raise NotImplementedError

    def delete_entries(self, entries, apply=False, **kwargs):  # pylint: disable=redefined-outer-name
        entries = [self._get_node(entry) for entry in entries]
        return self.delete_entry_objs(entries, apply=apply, **kwargs)

    def delete_entry_objs(self, entries, apply=False, **kwargs):  # pylint: disable=redefined-outer-name
        raise NotImplementedError

    # def _add_user(self, **data):
    #     raise NotImplementedError

    # def _delete_user(self, user, **kwargs):
    #     raise NotImplementedError

    def add_group(self, **data):
        group = self._make_node(self.group_cls, data)
        return self.add_group_obj(group) or group

    def add_group_obj(self, group, **kwargs):
        raise NotImplementedError

    def delete_group(self, group, **kwargs):
        group = self._get_node(group)
        return self.delete_group_obj(group, **kwargs)

    def delete_group_obj(self, group, **kwargs):
        raise NotImplementedError

    def get_node_permissions(self, node, **kwargs):
        node = self._get_node(node)
        return node.get_permissions(**kwargs)

    def set_node_permissions(self, node, permissions, **kwargs):
        permissions = dict(permissions)

        node = self._get_node(node)
        permissions = {
            perm_kind: [self._get_subject_node(subject) for subject in subjects]
            for perm_kind, subjects in permissions.items()}

        return node.set_permissions(permissions, **kwargs)

    def get_user_effective_groups(self, user, **kwargs):
        user = self._get_node(user)
        return user.get_effective_groups()

    def get_group_subjects(self, group, **kwargs):
        group = self._get_node(group)
        return group.get_subjects()

    def set_group_subjects(self, group, subjects, **kwargs):
        group = self._get_node(group)
        subjects = [self._get_subject_node(subject) for subject in subjects]
        return group.set_subjects(subjects, **kwargs)

    def add_group_subjects(self, group, subjects, **kwargs):
        group = self._get_node(group)
        subjects = [self._get_subject_node(subject) for subject in subjects]
        return group.add_subjects(subjects, **kwargs)

    def remove_group_subjects(self, group, subjects, **kwargs):
        group = self._get_node(group)
        subjects = [self._get_subject_node(subject) for subject in subjects]
        return group.remove_subjects(subjects, **kwargs)

""" Abstract core of a permission manager class """
from __future__ import annotations

from typing import Any, cast

from .utils import DocStringInheritor


__all__ = (
    'NodeBase',
    'EntryBase',
    'SubjectBase',
    'UserBase',
    'GroupBase',
    'DLSBase',
    'CycleFound',
)


class NodeBase:
    """ Common base class for a Node (Tree Node / Object / Entity) """

    id = None  # Required, GUID.
    name = None  # Optional, debug value.
    path = None  # Optional, alternative identifier to `id`.
    kind = None  # user / group / node
    scope = None  # system-specific node/entry 'type'

    def get_permissions(self, perm_kinds=None, **kwargs):
        """
        Get the ACLs for the specified `perm_kind`s (default: all).

        :returns: `{perm_kind: [subject, ...], ...}`
        """
        raise NotImplementedError


class EntryBase(NodeBase):
    """ A common base class for Entry (a non-special Node) """


class SubjectBase(NodeBase):
    """ A common base class for a Subject """


class UserBase(SubjectBase):
    """ A common base class for a User """

    def get_effective_groups(self):
        """
        Return a list of group objects in which the user belongs directly or
        indirectly.

        :returns: `[group, ...]`
        """
        raise NotImplementedError


class GroupBase(SubjectBase):
    """ A common base class for a Group """

    def get_subjects(self):
        """
        Return a list of subjects that belong to this group.

        :returns: `[subject, ...]`
        """
        raise NotImplementedError

    def set_subjects(self, subjects, **kwargs):
        raise NotImplementedError

    def add_subjects(self, subjects, **kwargs):
        raise NotImplementedError

    def remove_subjects(self, subjects, **kwargs):
        raise NotImplementedError


class CycleFound(Exception):
    """
    A special exception indicating that the groups graph contains a cycle.
    Checking for cycles is currently optional.
    """


class DLSBase(metaclass=DocStringInheritor):
    """
    Base (abstrat) definition of a DLS manager.
    """

    node_cls_base = node_cls = NodeBase
    entry_cls_base = entry_cls = EntryBase
    subject_cls_base = subject_cls = SubjectBase
    user_cls_base = user_cls = UserBase
    group_cls_base = group_cls = GroupBase

    special_perm_kinds_info = (
        dict(
            kind="acl_deny",
            description="A special `perm_kind` for denying access. Highest priority.",
        ),
        dict(
            kind="acl_adm",
            description="Node 'owner', full access.",
        ),
    )

    default_scope_info: dict[str, Any] = {
        "perm_kinds": ["acl_deny", "acl_adm", "acl_edit", "acl_view", "acl_execute"],
        "perm_kind_sizes": {
            # Essentially, 'how much permissions does it give'. Under the assumption that they are ordered.
            # Could be computed automatically from `actions`, but this is
            # getting set in stone, so not much point bothering.
            #
            #     scope_info['perm_kind_actions'] = groupby(
            #         (perm_kind, action)
            #         for action, perm_kinds in scope_info['actions']
            #         for perm_kind in perm_kinds)
            #     scope_info['perm_kind_sizes'] = {
            #         perm_kind: len(actions)
            #         for perm_kind, actions in scope_info['perm_kind_actions'].items()}
            #
            "acl_execute": 1, "acl_view": 2, "acl_edit": 3, "acl_adm": 4,
            # Too special: "acl_deny"
        },
        "perm_kind_titles": {
            # "acl_execute": "Исполнение",
            # "acl_view": "Просмотр",
            # "acl_edit": "Редактирование",
            # "acl_adm": "Администрирование",

            "acl_execute": "Execute",
            "acl_view": "Read",
            "acl_edit": "Write",
            "acl_adm": "Owner",
        },
        "actions": {
            "set_permissions": ["acl_adm"],
            # Can result in change of permissions at the path, therefore equal
            # to "set_permissions".
            "delete": ["acl_adm"],
            # Almost the same as 'delete A' + 'create_subnode B' but keeps the
            # identifier.
            "move": ["acl_adm"],

            "edit": ["acl_adm", "acl_edit"],
            "write": ["acl_adm", "acl_edit"],
            # To be checked on a folder when creating anything within it.
            "create_subnode": ["acl_adm", "acl_edit"],

            "read": ["acl_adm", "acl_edit", "acl_view"],
            "get_listing": ["acl_adm", "acl_edit", "acl_view"],
            # "copy" is same as "read A" + "create_subnode B".

            # "use the contents without seeing them"
            "execute": ["acl_adm", "acl_edit", "acl_view", "acl_execute"],
        },
        "__normalized": True,
    }
    special_scopes_info = {
        "user": default_scope_info,
        "group": default_scope_info,
        "system_folder": {
            **cast(dict, default_scope_info),
            "actions": {
                **cast(dict, default_scope_info['actions']),
                "create_subnode": ["acl_adm", "acl_edit"]}},
    }

    def __init__(self, **kwargs: Any):  # pylint: disable=unused-argument
        """ ... """

    def _get_scope_info(self, scope):
        raise NotImplementedError

    # ### Internal entrypoints ###

    def check_permission(self, user, node, action, **kwargs):
        """
        The main point of the system: whether a User `user` has access to doing
        an action `action` on the Node `node`.

        :param user: acting `User` node identifier.

        :param node: acted upon `Node` identifier.

        :param action: action name from the 'scopes' configuration.

        :returns: `is_allowed` (bool)
        """
        return self.check_permission_ext(user=user, node=node, action=action, **kwargs)['result']

    def check_permission_ext(self, user, node, action, **kwargs):
        """
        Same as `check_permission` but additionally returns metadata dict about
        the result.

        :returns: `{result: is_allowed, ...}`
        """
        raise NotImplementedError

    def check_permission_multi(self, user, action, requests, **kwargs):
        """
        Check permissions over multiple entries for a single user for the same action.

        :param user: `User` node identifier.

        :param requests: `[{"node": node, "_id": request-response matching value}, ...)`

        :returns: [{"_id": ..., "node": ..., "result": True|False}, ...]
        """
        results = []
        for piece in requests:
            result = self.check_permission(user=user, node=piece['node'], action=action, **kwargs)
            results.append(dict(piece, result=result, action=action))
        return results

    def check_permission_by_perm_kinds(self, user, node, perm_kinds, verbose=False, **kwargs):
        """
        ...

        Same as `check_permission_ext` but works on a prepared `perm_kinds` list,
        not an `action`.

        :returns: `{result: is_allowed, ...}`
        """
        raise NotImplementedError

    # ### Internal system management functions ###

    def get_configuration(self, name):
        """
        Get data of a `configuration` object.

        Known uses:

        A 'scopes' configuration object, in the format

        .. code-block:: text

          scope_name:
            # List of custom "permission kind" names for the scope (list of strings).
            # Note that `perm_kind`s 'acl_deny' and 'acl_adm' are always implicitly added.
            perm_kinds: [...]
            # Mapping from an `action` (by an user upon a node) to
            # `perm_kind`s that give access to the action.
            actions:
              action_name: [perm_kind, ...],
              ...
          ...

        """
        raise NotImplementedError

    def set_configuration(self, name, data):
        """
        Set a single configuration data.

        See :method get_configuration: for description of configurations.
        """
        raise NotImplementedError

    def add_entry(self, **data):
        """
        Add an Entry Node into the system.

        :param scope: system-specific scope. *Must* match a scope defined in
          the 'scopes' configuration object.

        :param id: Required. UUID of the node. Example: `"1ada32d2-4496-11e8-a3c8-525400123456"`.

        :param path: Optional. List of strings. An additional identifier interchangeable with the `id`.

        :param name: Optional. Name of the node for internal representation.
        """
        raise NotImplementedError

    def delete_entry(self, entry, apply=True, **kwargs):  # pylint: disable=redefined-outer-name
        """
        Delete a single Entry Node from the system.

        A simple wrapper over `delete_entries`.

        Does *not* do a dry run by default.

        :param entry: Entry Node identifier.
        """
        return self.delete_entries(entries=[entry], apply=apply, **kwargs)

    def delete_entries(self, entries, apply=False, **kwargs):  # pylint: disable=redefined-outer-name
        """
        Delete multiple entries from the system.

        :param entries: `[node_identifier, ...]`

        :param apply: whether to do any changes. NOTE: By default the method does a dry run.
        """
        raise NotImplementedError

    def _add_user(self, **data):
        """
        ...

        For internal use.
        """
        raise NotImplementedError

    def _delete_user(self, user, **kwargs):
        """
        ...

        For internal use.
        """
        raise NotImplementedError

    def add_group(self, **data):
        """
        Add a Group Node to the system.

        :param id: Required. UUID of the entry. Example: `"1ada32d2-4496-11e8-a3c8-525400123456"`.

        :param path: Optional. List of strings. An additional identifier interchangeable with the `id`.

        :param name: Optional. Name of the entry for internal representation.

        NOTE: there are special `path` values processed by the DLS code (see
        the system documentaion for details):

          * `[".groups", ".system", "superuser"]` - 'superuser' group.
          * `[".groups", ".system", "active"]` - 'active user' group.
        """
        raise NotImplementedError

    def delete_group(self, group, **kwargs):
        """
        Delete a Group Node from the system.

        :param group: node_identifier of a `Group` object.
        """
        raise NotImplementedError

    def get_node_permissions(self, node, **kwargs):
        """
        Get the user-facing permissions of the `node`.

        :param node: node identifier.

        :returns: `{perm_kind: [subject, ...], ...}`
        """
        raise NotImplementedError

    def set_node_permissions(self, node, permissions, **kwargs):
        """
        Set the user-facing permissions of the `node`.

        :param node: node identifier.

        :param permissions: `{perm_kind: [subject, ...], ...}`
        """
        raise NotImplementedError

    def get_user_effective_groups(self, user, **kwargs):
        """
        Get the denormalized list of user's groups.

        :param user: entity identifier of a `User` object.

        :returns: `[Group, ...]`
        """
        raise NotImplementedError

    def get_group_subjects(self, group, **kwargs):
        """
        Get the list of subjects that belong to a group.

        :param group: node identifier of a `Group` object.
        """
        raise NotImplementedError

    def set_group_subjects(self, group, subjects, **kwargs):
        """
        Set the list of subjects that belong to a group.

        :param group: node identifier of a group object.

        :param subjects: list of node identifier of subjects.
        """
        raise NotImplementedError

    def add_group_subjects(self, group, subjects, **kwargs):
        """
        Add to the list of subjects that belong to a group.

        :param group: node identifier of a group object.

        :param subjects: list of node identifiers of subjects.
        """
        raise NotImplementedError

    def remove_group_subjects(self, group, subjects, **kwargs):
        """
        Remove from o the list of subjects that belong to a group.

        :param group: node identifier of a group object.

        :param subjects: list of node identifiers of subjects.
        """
        raise NotImplementedError

# coding: utf8
"""
Reusable base parts of the DLS manager structure.
"""

__all__ = (
    'common_structure_base',
    'common_structure',
    'structure_to_reqs',
    'load_structure',
)


# # printf "id='%s'" "$(uuid)"

# System entities structure (special DLS code support):
common_structure_base = (
    dict(kind="configuration",
         name="scopes",
         data={
             "config": {  # scope named "config"
                 "perm_kinds": ["acl_edit", "acl_view"],
                 "actions": {
                     "write": ["acl_adm", "acl_edit"],
                     "read": ["acl_adm", "acl_edit", "acl_view"],
                 },
             },
             "folder": {  # scope named "folder"
                 "perm_kinds": ["acl_edit", "acl_view"],
                 "actions": {
                     "create_subnode": ["acl_adm", "acl_edit"],
                     "get_listing": ["acl_adm", "acl_edit", "acl_view"],
                 },
             },
         }),
    # ## Base folders structure ##
    dict(kind="entry",
         scope="system_folder",
         id="e113722c-447a-11e8-90c5-525400123456",
         path=[".groups"],
         __node_permissions=dict(
             acl_edit=[
                 [".system_groups", "group_manager"],
             ],
         )),
    dict(kind="entry",
         scope="system_folder",
         id="44b72a9c-448c-11e8-9403-525400123456",
         path=[".system_groups"]),
    dict(kind="entry",
         scope="system_folder",
         id="5997671e-448d-11e8-ad58-525400123456",
         path=[".users"]),
    dict(kind="entry",
         scope="system_folder",
         id='43bb7eda-4852-11e8-88e6-525400123456',
         path=[".system_users"]),

    # # Users: not listed, should be created automatically.
    # dict(kind='user',
    #      id="e6beb1-b350-40c1-9c20-fca44627588d",
    #      path=[".system_users", "superuser"]),

    # ## System Groups ##
    # Superuser. Internally supported group that allows all actions.
    dict(kind='group',
         id="ec2db41f-828e-432b-ab0f-0c55d8f64fc9",
         path=[".system_groups", "superuser"],
         __group_subjects=[
             [".system_users", "superuser"],
         ]),
    # Groups that determine access to creation of new groups / users.
    dict(kind='group',
         id='ba4ec5da-4851-11e8-beab-525400123456',
         path=[".system_groups", "group_manager"]),
    dict(kind='group',
         id='c13148ea-4870-11e8-9db7-525400123456',
         path=[".system_groups", "user_manager"]),
    # "Active users". Internally supported group that is required for any
    # action (except for superusers). Can also be used as alias for "all users".
    dict(kind='group',
         id="93748008-447d-11e8-ae02-525400123456",
         path=[".system_groups", "active"],
         __group_subjects=[
             [".system_users", "superuser"],
         ]),
)


# TODO: DL-Common entities structure (special DL code support)


# A commonly useful entities structure (no special code support):
common_structure = common_structure_base + (
    # ## Folders ##
    dict(kind="entry",
         scope="folder",
         id="1a9952d0-4491-11e8-9463-525400123456",
         path=["root"],
         # Folder, listing is available to all users.
         __node_permissions=dict(
             acl_view=[
                 [".system_groups", "active"],
             ],
         )),
    dict(kind="entry",
         scope="folder",
         id="532b9740-74a1-4415-90a1-d1231567f7c1",
         path=["root", "home"],
         # Folder, listing is available to all users.
         __node_permissions=dict(
             acl_view=[
                 [".system_groups", "active"],
             ],
         )),
    dict(kind="entry",
         scope="folder",
         id="f676d3b0-448a-11e8-801a-525400123456",
         path=["root", "tmp"],
         # Folder, listing and creating subnodes is available to all users.
         __node_permissions=dict(
             acl_edit=[
                 [".system_groups", "active"],
             ],
         )),

    # ## "Special" groups ##
    dict(kind="group",
         id="af6ced40-e439-4394-9822-4ca56b2aa6d4",
         path=[".groups", "manager"],
         __belongs_to_groups=[
             # TODO: should include some DL-Common groups.
             [".system_groups", "group_manager"],
         ]),
)


def structure_to_reqs(structure, inplace=False, apply_to_mgr=None):  # pylint: disable=too-many-locals
    reqs = []

    def add_req(item, method, params):  # pylint: disable=redefined-outer-name
        req = dict(method=method, params=params)
        reqs.append(req)
        if apply_to_mgr is not None:
            mgr_method = getattr(apply_to_mgr, req['method'])
            mgr_method(**req['params'])

        return req

    node_keys = set(("id", "path", "name", "scope"))

    for item in structure:
        item = dict(item)
        kind = item['kind']

        if kind == 'configuration':
            add_req(item=item, method="set_configuration",
                    params=dict(name=item['name'], data=item['data']))
            continue
        elif kind == 'user':
            # Should be autocreated.
            continue

        method = "add_{}".format(kind)
        params = {key: value for key, value in item.items() if key in node_keys}
        add_req(item=item, method=method, params=params)

    # On a second pass, apply the modifications:
    for item in structure:
        if item['kind'] in ["configuration"]:
            continue

        item_id = item.get('path') or item['id']

        group_subjects = item.get('__group_subjects')
        if group_subjects:
            add_req(item=item, method='set_group_subjects', params=dict(
                group=item_id,
                subjects=group_subjects))

        # A reverse add-subject-to-group, for convenience.
        belongs_to_groups = item.get('__belongs_to_groups')
        if belongs_to_groups:
            for group in belongs_to_groups:
                add_req(item=item, method='add_group_subjects', params=dict(
                    group=group, subjects=[item_id]))

        node_permissions = item.get('__node_permissions')
        if node_permissions:
            add_req(item=item, method='set_node_permissions', params=dict(
                node=item_id, permissions=node_permissions))

    return reqs


def load_structure(mgr, structure, inplace=False):
    return structure_to_reqs(structure, inplace=inplace, apply_to_mgr=mgr)

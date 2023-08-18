# coding: utf8
"""
...
"""


from __future__ import annotations

from pytest import fixture

from bi_utils.utils import DotDict

from yadls.commons import common_structure, load_structure
from yadls.manager_inmem import DLSInmem


test_structure = common_structure + (
    # Test Users
    dict(kind="user",
         id="0290ed1f-90d7-4272-aef0-a73d28a3e5bb",
         path=[".users", "user1"],
         __belongs_to_groups=[[".system_groups", "active"]]),
    dict(kind="user",
         id="b06bc620-43de-11e8-ba55-525400123456",
         path=[".users", "user2"],
         __belongs_to_groups=[[".system_groups", "active"]]),
    dict(kind="user",
         id="244667f2-4494-11e8-8825-525400123456",
         path=[".users", "user3"],
         __belongs_to_groups=[[".system_groups", "active"]]),

    # # A non-active user.
    # dict(kind="user",
    #      id="566b6606-448a-11e8-b391-525400123456",
    #      path=[".users", "former_user"],
    #      __belongs_to_groups=[]),

    # Sample Groups
    # The point of a system group is to check whether the current user
    # belongs to it, from the interface and main API, when deciding whether
    # some code-supported action is allowed (e.g. creation of a config).
    dict(kind="group",
         id="a371049a-cdc9-4f87-bc01-3a5aef1a03e3",
         path=[".system_groups", "some_system_group"],
         __group_subjects=[
             [".groups", "manager"],
         ]),

    # Test Folders
    dict(kind="entry",
         scope="folder",
         id="49ffb90e-447a-11e8-83e3-525400123456",
         path=["root", "home", "user1"],
         __node_permissions=dict(acl_adm=[[".users", "user1"]])),
    dict(kind="entry",
         scope="config",
         id="bd7dc524-447a-11e8-81ff-525400123456",
         path=["root", "home", "user1", "some_config"],
         # `user1`"s object that is readable to all but `user2`.
         __node_permissions=dict(
             acl_adm=[[".users", "user1"]],
             acl_deny=[[".users", "user2"]],
             acl_view=[[".system_groups", "active"]])),
    dict(kind="entry",
         scope="folder",
         id="1ada32d2-4496-11e8-a3c8-525400123456",
         path=["root", "home", "user2"],
         node_permissions=dict(acl_adm=[[".users", "user2"]])),
)


@fixture
def world():
    mgr = DLSInmem()
    result = dict(mgr=mgr)
    result['nodes'] = load_structure(mgr, test_structure)
    return DotDict(result)


def _check_some(user, node, action, world, last_meta, **kwargs):  # pylint: disable=redefined-outer-name
    results = world.mgr.check_permission_ext(
        user=user, node=node, action=action,
        verbose=True, **kwargs)
    last_meta.clear()
    last_meta.update(results)
    return results['result']


test_stuff_checks = (
    dict(user=[".system_users", "superuser"], node=[".groups"],
         action="set_permissions", result=True,
         comment="superuser should be allowed anything"),
    dict(user=[".users", "user2"], node=["root", "home"],
         action="get_listing", result=True,
         comment="home folder is set to viewable to all users"),
    # # This check is effectively disabled (because of the sync lag):
    # dict(user=[".users", "former_user"], node=["root", "home"],
    #      action="get_listing", result=False,
    #      comment="inactive user should have no rights"),
    dict(user=[".users", "user1"], node=["root", "home"],
         action="set_permissions", result=False,
         comment="no such right should be there"),
    dict(user=[".users", "user1"], node=["root", "home", "user1"],
         action="set_permissions", result=True,
         comment="user1 should have full access to own home folder"),

    dict(user=[".users", "user1"], node=["root", "home", "user1", "some_config"],
         action="set_permissions", result=True,
         comment="user1 should have full access to an owned object"),
    dict(user=[".users", "user1"], node=["root", "home", "user1", "some_config"],
         action="read", result=True,
         comment="user1 should have full access to an owned object"),
    dict(user=[".users", "user3"], node=["root", "home", "user1"],
         action="get_listing", result=False, comment="no such right"),
    dict(user=[".users", "user3"], node=["root", "home", "user1", "some_config"],
         action="read", result=True, comment="allow_all"),
    dict(user=[".users", "user2"], node=["root", "home", "user1", "some_config"],
         action="read", result=False, comment="acl_deny"),
    dict(user=[".users", "user3"], node=["root", "home", "user1", "some_config"],
         action="write", result=False, comment="no such right"),
)


def test_stuff(world):  # pylint: disable=redefined-outer-name
    last_meta = {}

    # NOTE: could make this into a parametrized tests, but would need to
    # somehow skip the `world` fixture building.
    for info in test_stuff_checks:
        result = _check_some(
            user=info['user'], node=info['node'], action=info['action'],
            world=world, last_meta=last_meta,
            assert_check_result=info['result'])
        assert result is info['result'], (info['comment'], last_meta)


def test_cyclic_groups(world):  # pylint: disable=redefined-outer-name
    last_meta = {}
    mgr = world.mgr
    node = ["root", "home", "user2"]
    g1 = mgr.add_group(path=[".groups", "grp1"])
    g2 = mgr.add_group(path=[".groups", "grp2"])
    g3 = mgr.add_group(path=[".groups", "grp3"])
    mgr.set_node_permissions(node, dict(acl_view=[[".groups", "grp3"]]))
    mgr.set_group_subjects(g1, [[".groups", "grp2"], [".users", "user2"]])
    mgr.set_group_subjects(g2, [[".groups", "grp3"]])
    mgr.set_group_subjects(g3, [[".groups", "grp1"]])
    assert _check_some(
        user=[".users", "user2"], node=node, action="get_listing",
        world=world, last_meta=last_meta) is True, ("group-clique", last_meta)

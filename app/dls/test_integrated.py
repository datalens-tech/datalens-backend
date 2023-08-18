#!/usr/bin/env python3
"""
Recommended invocations:

    py.test test_integrated.py -vvs --log-cli-level 1  --pdb

Over beta environment (to skip the setup):

    ./_envit ./.env_int_beta sh -c 'DLS_HTTP_PORT=38080 DLS_IPDBG=1 python -m yadls.httpjrpc.main'
    # or: `./app/bidls_app bidls_devhttp`
    # In a different console:
    ./_envit ./.env_int_beta sh -c 'DLS_HOST=http://localhost:38080/_dls/ py.test test_integrated.py -vvs --log-cli-level 1  --pdb'

"""

from __future__ import annotations

import os
import uuid
import logging
import time
import json
import random as base_random
# from urllib.parse import urljoin
from collections import namedtuple

import requests

# from pyaux.base import dict_is_subset

# pylint: disable=unused-import
try:
    from pyaux.madness import p_datadiff, _yprint  # conveniences
except Exception:
    pass


# NOTE:
requests.packages.urllib3.disable_warnings()

HOST = os.environ.get('DLS_HOST') or 'http://[::1]:38080/_dls'
API_KEY = os.environ.get('DLS_API_KEY') or ''
PERF_PERM_CHECK = os.environ.get('DLS_TST_PERF_PERM_CHECK')

DEFAULT_ACTIONS = ('set_permissions', 'delete', 'edit', 'read')
RND_SEED = os.environ.get('RND_SEED') or None
RANDOM = base_random.Random(RND_SEED)
TEST_TAG = '%08x' % (RANDOM.getrandbits(8 * 4), )

# # Might be subject to change:
# EMPTY_PERMISSIONS = {key: [] for key in ['acl_deny', 'acl_adm', 'acl_edit', 'acl_view']}
EMPTY_PERMISSIONS = {}

LOGGER = logging.getLogger('test_integrated')
SESSION = requests.Session()


ROOT_USER = 'system_user:root'

# TODO: initial_inherited=True, inherited_from=...
INITIAL_INHERITED_MARKER = dict(extras=dict(initial_on_create=True), description='')
INITIAL_INHERITED = dict(INITIAL_INHERITED_MARKER, requester=ROOT_USER, approver=ROOT_USER)

# TODO: initial_for_creator=True
INITIAL_OWNER_MARKER = dict(extras=dict(initial_on_create=True), description='')
INITIAL_FOR_OWNER = dict(INITIAL_OWNER_MARKER, requester=ROOT_USER, approver=ROOT_USER)

# TODO: initial_explicit=True
INITIAL_EXPLICIT_MARKER = dict(extras=dict(initial_on_create=True), description='')


def make_uuid():
    return str(uuid.uuid4()) # XXXX: unicode: + '⋯'


def make_user(idx):
    # XXXX: unicode: return 'user:inuser⋯%s_%02d' % (TEST_TAG, idx)
    return 'user:inuser_%s_%02d' % (TEST_TAG, idx)


def log_resp(resp, **kwargs):
    if not os.environ.get('DBG_LOG_RESP'):
        return
    data = dict(
        kwargs,
        method=resp.request.method,
        timing=resp.elapsed.total_seconds(),
        status=resp.status_code,
    )
    data_s = json.dumps(data) + '\n'
    with open('_test_resps.log', 'a', buffering=1) as fobj:
        fobj.write(data_s)


# pylint: disable=redefined-outer-name
def req(path, params=None, *, json=None, data=None, requester=None, method=None, verify=True, _rfs=True, **kwargs):
    headers = {
        'X-API-Key': API_KEY,
        'X-DL-Allow-Superuser': '1',
        'X-YaCloud-CloudId': '-',
    }
    if requester is not None:
        headers['X-User-Id'] = requester.encode('utf-8')
    headers.update(kwargs.pop('headers', None) or {})

    realm = 'intest_{}'.format(TEST_TAG)
    params = params or {}
    params.setdefault('realm', realm)
    params.setdefault('_force_db_master', '1')

    # uri = urljoin(HOST, path)
    # # Problem: most of the paths are '/'-prefixed now, which in urljoin terms is 'from root'.
    uri = '{}{}'.format(HOST, path)
    if json is not None and data is not None:
        raise TypeError("Don't mix `json` and `data`.")
    if method is None:
        method = 'post' if json is not None or data is not None else 'get'
    resp = SESSION.request(
        method, uri, params=params, json=json, headers=headers, verify=verify,
        **kwargs)
    log_resp(resp, path=path)
    if _rfs:
        resp.raise_for_status()
    return resp


def req_and_check(path, params=None, *, expected_status=200, **kwargs):
    resp = req(path=path, params=params, _rfs=False, **kwargs)
    assert resp.status_code == expected_status, resp.text
    return resp.json()


SimplePermsState = namedtuple('SimplePermsState', ('perms', 'pending', 'editable'))


def get_permissions_simplifed(node_uuid, requester):
    data = req_and_check('nodes/all/{}/permissions'.format(node_uuid), requester=requester)
    result = tuple((
        {perm_kind: [subject['name'] for subject in subjects]
         for perm_kind, subjects in data[key].items()
         if subjects}
        for key in ('permissions', 'pendingPermissions')))
    return SimplePermsState(*result, *(data['editable'],))


def check_permissions(user, nodes, action='read', force_multi=False, verbose=False, **kwargs):
    req_data = dict(user=user, action=action, nodes=nodes)
    if verbose:
        req_data['verbose'] = verbose
    if len(nodes) != 1 or force_multi:
        data = req_and_check('batch_accesses', json=req_data, **kwargs)
        return data
    # elif len(nodes) == 1 and not force_multi:
    node = nodes[0]
    params = dict(kwargs.pop('params', None) or {}, user=user)
    data = req_and_check(
        'nodes/all/{}/access/{}'.format(node, action),
        params=params, **kwargs)
    return {node: data}


def check_permissions_simple(user, nodes, action='read', **kwargs):
    result = check_permissions(user=user, nodes=nodes, action=action, **kwargs)
    return {node: item['result'] for node, item in result.items()}


def add_one_diff(user, comment="(intest add one)", perm_kind="acl_adm"):
    return {"diff": {"added": {perm_kind: [{"subject": user, "comment": comment}]}, "removed": {}}}


def remove_one_diff(user, comment="(intest remove one)", perm_kind="acl_adm"):
    return {"diff": {"added": {}, "removed": {perm_kind: [{"subject": user, "comment": comment}]}}}


def modify_one_diff(perm_kind, user, new_data, comment="(intest modify one)"):
    return {"diff": {"added": {}, "removed": {}, "modified": {perm_kind: [
        {"subject": user, "comment": comment, "new": new_data}]}}}


def simplify_history_item(item, parts=('subject', 'perm_kind', 'author', 'state', 'prev_state', 'situation', 'dedup')):
    part_to_getter = dict(
        author=lambda item: item['author']['name'],
        subject=lambda item: item['grant']['subject']['name'],
        active=lambda item: item['grant']['active'],
        state=lambda item: item['grant']['state'],
        prev_state=lambda item: item['previous'].get('state'),
        description=lambda item: item['grant']['description'],
        perm_kind=lambda item: item['grant']['grantType'],
        dedup=lambda item: item['extras'].get('dedup'),
    )
    result = {}
    for part in parts:
        getter = part_to_getter.get(part)
        if getter is not None:
            value = getter(item)
        else:
            value = item[part]
        if value is not None:
            result[part] = value
    return result


def get_history_simplified(node_uuid, subject, requester, **kwargs):
    data = req_and_check('nodes/all/{}/permissions/subjects/{}'.format(node_uuid, subject), requester=requester)
    items = data['history']
    items = [simplify_history_item(item, **kwargs) for item in items]
    return items


CNT = 8


def prepare():
    uuids = [make_uuid() for _ in range(CNT)]
    LOGGER.info("%s", ", ".join("uuids[%r] = %r" % (idx, uuid) for idx, uuid in enumerate(uuids)))

    users = [make_user(idx=idx) for idx in range(CNT)]
    LOGGER.info("%s", ", ".join("users[%r] = %r" % (idx, user) for idx, user in enumerate(users)))

    root_node = make_uuid()

    # node0, root, user0
    nodeinfo_00 = req_and_check('nodes/entries/{}'.format(root_node), method='put', json=dict(
        scope='whateverA', initialOwner=users[0],
        initialPermissionsMode='owner_only'))
    LOGGER.info("nodeinfo_00: %r", nodeinfo_00)

    for idx in range(CNT):
        # nodeX, under node0, userX
        nodeinfo = req_and_check('nodes/entries/{}'.format(uuids[idx]), method='put', json=dict(
            scope='whateverB', initialOwner=users[idx], initialParent=root_node,
            initialPermissionsMode='parent_and_owner'))
        LOGGER.info("nodeinfo_%r: %r", idx, nodeinfo)

    return dict(uuids=uuids, users=users, root_node=root_node)


def subtest_dedup(users):
    uuid_dd = make_uuid()
    uri_perms = 'nodes/all/{}/permissions'.format(uuid_dd)
    owner = users[0]
    target = users[1]
    nodeinfo_dd = req_and_check(
        'nodes/entries/{}'.format(uuid_dd),
        method='put',
        json=dict(
            scope='whateverY',
            initialOwner=owner,
            initialPermissionsMode='owner_only'))
    LOGGER.info("nodeinfo_dd: %r", nodeinfo_dd)

    # Just added
    req_and_check(
        uri_perms, requester=owner, method='patch',
        json=add_one_diff(target, perm_kind='acl_view'))
    perms, pending, editable = get_permissions_simplifed(uuid_dd, requester=owner)
    assert editable
    # assert not pending
    assert pending == EMPTY_PERMISSIONS, pending
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[owner], acl_view=[target]), perms

    # Skip acl_execute when acl_view exists
    req_and_check(uri_perms, requester=owner, method='patch', json=add_one_diff(target, perm_kind='acl_execute'))
    perms, pending, editable = get_permissions_simplifed(uuid_dd, requester=owner)
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[owner], acl_view=[target]), perms

    history = get_history_simplified(node_uuid=uuid_dd, subject=target, requester=owner)
    assert history[0] == dict(
        subject=target, author=owner,
        situation='some_request_ignored_because_of_existing_larger',
        # This is a dubious hack, as the requested permission is not active
        # (ignored), and active permission is 'acl_view'.
        perm_kind='acl_execute', state='active',
        dedup=dict(pre='acl_view', req='acl_execute', res='acl_view'))

    # Rewrite acl_view with acl_edit
    req_and_check(uri_perms, requester=owner, method='patch', json=add_one_diff(target, perm_kind='acl_edit'))
    perms, pending, editable = get_permissions_simplifed(uuid_dd, requester=owner)
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[owner], acl_edit=[target]), perms

    history = get_history_simplified(node_uuid=uuid_dd, subject=target, requester=owner)
    history = sorted(history[:2], key=lambda item: item['perm_kind'])
    expected = [
        dict(
            subject=target, author=owner,
            situation='grant_new_add',
            perm_kind='acl_edit', state='active'),
        dict(
            subject=target, author=owner,
            situation='grant_override_by_larger',
            perm_kind='acl_view', prev_state='active', state='deleted',
            dedup=dict(pre='acl_view', req='acl_edit', res='acl_edit')),
    ]
    assert history == expected

    # Skip request of acl_execute when acl_edit exists:
    req_and_check(uri_perms, requester=target, method='patch', json=add_one_diff(target, perm_kind='acl_execute'))
    perms, pending, editable = get_permissions_simplifed(uuid_dd, requester=owner)
    assert pending == EMPTY_PERMISSIONS, pending
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[owner], acl_edit=[target]), perms

    # Request acl_adm
    req_and_check(uri_perms, requester=target, method='patch', json=add_one_diff(target, perm_kind='acl_adm'))
    perms, pending, editable = get_permissions_simplifed(uuid_dd, requester=owner)
    assert pending == dict(EMPTY_PERMISSIONS, acl_adm=[target])
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[owner], acl_edit=[target]), perms

    # Deny acl_adm
    req_and_check(uri_perms, requester=owner, method='patch', json=remove_one_diff(target, perm_kind='acl_adm'))
    perms, pending, editable = get_permissions_simplifed(uuid_dd, requester=owner)
    assert pending == EMPTY_PERMISSIONS, pending
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[owner], acl_edit=[target]), perms



def subtest_suggest(users):
    for search_text in ('inuser', 'inuser '):
        res = req_and_check(
            'suggest/subjects/',
            requester=users[0],
            params=dict(search_text=search_text, limit=4),
            headers={'X-User-Language': 'en'},
        )
        assert isinstance(res['results'], list)


def subtest_perf_perm_check(uuids, users, actions=DEFAULT_ACTIONS, duration=10.0):
    start_time = time.time()
    max_time = start_time + duration
    results = []
    while True:
        if time.time() >= max_time:
            break
        node = RANDOM.choice(uuids)
        user = RANDOM.choice(users)
        action = RANDOM.choice(actions)
        t1 = time.time()
        check_permissions(user=user, nodes=[node], action=action)
        t2 = time.time()
        results.append(t2 - t1)
    LOGGER.info(
        "Timing results cnt=%d, avg=%.3f, min=%.3f, max=%.3f, data=%s",
        len(results), sum(results) / len(results), min(results), max(results), json.dumps(results))
    return results


def test_main():

    world = prepare()
    root_node = world['root_node']
    users = world['users']
    uuids = world['uuids']

    perms, pending, editable = get_permissions_simplifed(root_node, requester=users[0])
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[users[0]]), perms
    assert pending == EMPTY_PERMISSIONS, pending
    assert editable

    perms, pending, editable = get_permissions_simplifed(uuids[1], requester=users[0])
    # `users[0]` should've been inherited:
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[users[0], users[1]]), perms
    assert pending == EMPTY_PERMISSIONS, pending
    assert editable

    perms, pending, editable = get_permissions_simplifed(uuids[2], requester=users[3])
    # `users[0]` should've been inherited:
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[users[0], users[2]]), perms
    assert pending == EMPTY_PERMISSIONS, pending
    assert not editable

    # Modify by owner check:
    res = req_and_check(
        'nodes/all/{}/permissions'.format(uuids[1]), requester=users[0], method='patch',
        json=dict(diff=dict(
            added=dict(acl_view=[dict(subject=users[2], comment='add1')]),
            removed=dict(acl_adm=[dict(subject=users[0])]),
        )))
    LOGGER.info("modify1 res: %r", res)

    # Check that the modification result is as expected
    perms, pending, editable = get_permissions_simplifed(uuids[1], requester=users[0])
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[users[1]], acl_view=[users[2]]), perms

    assert check_permissions_simple(user=users[0], nodes=[root_node, uuids[1]], action='read') == {root_node: True, uuids[1]: False}
    assert check_permissions_simple(user=users[1], nodes=[uuids[2]], action='read') == {uuids[2]: False}

    # # Check the realm check
    # assert check_permissions_simple(user=users[0], nodes=[root_node, uuids[1]], action='read', params=dict(realm='wrong_realm')) == {root_node: False, uuids[1]: False}
    # assert check_permissions_simple(user=users[0], nodes=[root_node], action='read', params=dict(realm='wrong_realm')) == {root_node: False}

    assert check_permissions_simple(user=users[0], nodes=[root_node, uuids[1]], action='read') == {root_node: True, uuids[1]: False}

    # # Shorthands:
    # add_one_diff = lambda user, comment="(intest add one)", perm_kind="acl_adm": {
    #     "diff": {"added": {perm_kind: [{"subject": user, "comment": comment}]}, "removed": {}}}
    # remove_one_diff = lambda user, comment="(intest remove one)", perm_kind="acl_adm": {
    #     "diff": {"added": {}, "removed": {perm_kind: [{"subject": user, "comment": comment}]}}}
    # modify_one_diff = lambda perm_kind, user, new_data, comment="(intest modify one)": {
    #     "diff": {"added": {}, "removed": {}, "modified": {perm_kind: [
    #         {"subject": user, "comment": comment, "new": new_data}]}}}

    def simplify_perms(perms, keys=('name', 'description', 'requester', 'approver', 'extras')):

        def preprocess_item(item):
            if item.get('subject'):
                item = item.copy()
                item['name'] = item.pop('subject')
            if not item.get('extras'):  # empty -> drop for convenience
                item.pop('extras', None)
            return item

        perms = {
            perm_kind: sorted(
                [{key: (val['name'] if isinstance(val, dict) and 'name' in val
                        else val)
                  for key, val in preprocess_item(subj).items()
                  if key in keys and val is not None}
                 for subj in subjs],
                key=lambda item: item['name'])
            for perm_kind, subjs in perms.items()
            if subjs}
        return perms

    uri2 = 'nodes/all/{}/permissions'.format(uuids[2])

    #  * request permission while non-editable
    assert not get_permissions_simplifed(uuids[2], requester=users[3]).editable
    actions = (
        # # * add acl_adm users[6], by users[6]
        dict(requester=users[6], subject=users[6], comment='for self', perm_kind='acl_adm'),
        # # * add acl_view users[3], by users[4]
        dict(requester=users[4], subject=users[3], comment='for another', perm_kind='acl_view'),
        # # * add acl_adm users[3], by users[4] (atop existing pending) -> 400 already exists.
        # dict(requester=users[4], subject=users[3], comment='i concur', perm_kind='acl_adm'),
        # # * add acl_edit users[4], by users[3]
        dict(requester=users[3], subject=users[4], comment='reciprocate', perm_kind='acl_edit'),
        # # * add acl_adm users[5], by users[6]
        dict(requester=users[6], subject=users[5], comment='for another p2', perm_kind='acl_adm'),
        # # * add acl_adm users[3], by users[6]
        dict(requester=users[6], subject=users[2], comment='request over existing', perm_kind='acl_adm'),
    )

    for action in actions:
        req_and_check(
            uri2, method='patch', requester=action['requester'],
            json={
                "diff": {
                    "added": {
                        action['perm_kind']: [
                            {"subject": action['subject'], "comment": action['comment']}
                        ]},
                    "removed": {}}})

    state = req_and_check(uri2, requester=users[3])
    assert not state['editable']
    # Should be basically unchanged:
    _expected = dict(EMPTY_PERMISSIONS, acl_adm=[
        dict(INITIAL_INHERITED, name=users[0]),
        dict(INITIAL_FOR_OWNER, name=users[2]),
    ])
    assert simplify_perms(state['permissions']) == simplify_perms(_expected), state['permissions']
    # All that stuff:
    _expected = dict(
        EMPTY_PERMISSIONS,
        acl_adm=[
            # dict(name=users[3], requester=users[3], description='for self | i concur'),
            dict(name=users[6], requester=users[6], description='for self'),
            dict(name=users[5], requester=users[6], description='for another p2'),
        ],
        acl_edit=[
            dict(name=users[4], requester=users[3], description='reciprocate'),
        ],
        acl_view=[
            dict(name=users[3], requester=users[4], description='for another'),
        ],
    )
    # * check that they ended up in pendingPermissions
    assert simplify_perms(state['pendingPermissions']) == simplify_perms(_expected), state['pendingPermissions']

    #  * fail revoking the request by an irrelevant user: remove acl_edit users[3], by users[6]
    res = req_and_check(
        uri2, method='patch', requester=users[6],
        json=remove_one_diff(users[4], perm_kind='acl_edit'),
        expected_status=403)
    # assert res['grant_data']  # nope.
    assert res['existing_grant']
    assert res['message']

    #  * revoke the request by the requester: remove acl_view users[3], by users[4]
    req_and_check(
        uri2, method='patch', requester=users[4],
        json=remove_one_diff(users[3], perm_kind='acl_view'))
    #  * revoke the request by the target: remove acl_adm users[5] by users[5]
    req_and_check(
        uri2, method='patch', requester=users[5],
        json=remove_one_diff(users[5], perm_kind='acl_adm'))
    #  * confirm the request by an owner: add acl_adm users[3] by users[2]
    req_and_check(
        uri2, method='patch', requester=users[2],
        json=add_one_diff(users[6], perm_kind='acl_adm'))

    state = req_and_check(uri2, requester=users[0])

    _expected = dict(EMPTY_PERMISSIONS, acl_adm=[
        dict(INITIAL_INHERITED, name=users[0]),
        dict(INITIAL_FOR_OWNER, name=users[2]),
        dict(name=users[6],
             # description='for self | i concur',
             description='for self',
             approver=users[2], requester=users[6]),
    ])
    assert simplify_perms(state['permissions']) == simplify_perms(_expected), state['permissions']

    _expected = dict(
        EMPTY_PERMISSIONS,
        acl_edit=[
            dict(name=users[4], requester=users[3], description='reciprocate'),
        ],
    )
    assert simplify_perms(state['pendingPermissions']) == simplify_perms(_expected), state['pendingPermissions']

    # test 'remove, add' and 'remove, add with different grantType'
    req_and_check(
        uri2, method='patch', requester=users[2],
        json=add_one_diff(users[3], perm_kind='acl_view'))
    req_and_check(
        uri2, method='patch', requester=users[2],
        json=add_one_diff(users[5], perm_kind='acl_view'))

    # test 'modify permissions'

    req_and_check(
        uri2, method='patch', requester=users[2],
        json=modify_one_diff(perm_kind='acl_view', user=users[5], new_data=dict(
            description='The other one', subject=users[4])))
    perms, pending, editable = get_permissions_simplifed(uuids[2], requester=users[0])
    _expected = dict(
        EMPTY_PERMISSIONS,
        acl_adm=[users[0], users[2], users[6]],
        acl_view=[users[3], users[4]],
    )
    assert perms == _expected, perms

    uri3 = 'nodes/all/{}/permissions'.format(uuids[3])
    perms, pending, editable = get_permissions_simplifed(uuids[3], requester=users[0])
    assert editable
    assert not pending
    assert perms == dict(EMPTY_PERMISSIONS, acl_adm=[users[0], users[3]]), perms

    req_and_check(
        uri3, method='patch', requester=users[0],
        json=modify_one_diff(perm_kind='acl_adm', user=users[3], new_data=dict(
            description='Demoted permissions', grantType='acl_view')))
    state = req_and_check(uri3, requester=users[3])
    _expected = dict(
        EMPTY_PERMISSIONS,
        acl_adm=[dict(INITIAL_INHERITED, name=users[0])],
        acl_view=[dict(INITIAL_FOR_OWNER, name=users[3], description='Demoted permissions')],
    )
    assert simplify_perms(state['permissions']) == simplify_perms(_expected), state['permissions']

    # Check: modify by the bearer is forbidden:
    req_and_check(
        uri3, method='patch', requester=users[3],
        json=modify_one_diff(perm_kind='acl_view', user=users[3], new_data=dict(
            description='Give them back', grantType='acl_adm')),
        expected_status=403)

    # # modify non-editable: add a new pending grant?
    # req_and_check(
    #     uri3, method='patch', requester=users[3],
    #     json=modify_one_diff(perm_kind='acl_view', user=users[3], new_data=dict(
    #         description='Please give them back', grantType='acl_adm')))
    # state = req_and_check(uri3, requester=users[3])
    # _expected_pending = dict(
    #     EMPTY_PERMISSIONS,
    #     acl_view=[dict(name=users[3], description='Please give them back')],
    # )
    # assert simplify_perms(state['permissions']) == simplify_perms(_expected), state['permissions']
    # assert simplify_perms(state['pendingPermissions']) == simplify_perms(_expected_pending), state['pendingPermissions']

    # request a new grantType:
    req_and_check(
        uri3, method='patch', requester=users[3],
        json=dict(diff=dict(
            added=dict(acl_adm=[dict(subject=users[3], comment='Please give them back')]),
            removed=dict(),
        )))
    state = req_and_check(uri3, requester=users[3])
    _expected_pending = dict(
        EMPTY_PERMISSIONS,
        acl_adm=[dict(name=users[3], requester=users[3], description='Please give them back')],
    )
    assert simplify_perms(state['permissions']) == simplify_perms(_expected), state['permissions']
    assert simplify_perms(state['pendingPermissions']) == simplify_perms(_expected_pending), state['pendingPermissions']

    # Reject and add new in one request.
    # TODO?: modify-and-approve?
    req_and_check(
        uri3, method='patch', requester=users[0],
        json=dict(diff=dict(
            added=dict(acl_edit=[dict(subject=users[3], comment='Only this much')]),
            removed=dict(acl_adm=[dict(subject=users[3], comment='Giving edit instead')]),
        )))
    _expected = dict(
        EMPTY_PERMISSIONS,
        acl_adm=[dict(INITIAL_INHERITED, name=users[0])],
        acl_edit=[dict(name=users[3], description='Only this much', approver=users[0], requester=users[0])],
        # # Pre-dedup:
        # acl_view=[dict(INITIAL_FOR_OWNER, name=users[3], description='Demoted permissions')],
        # # Dedup:
        acl_view=[],
    )
    state = req_and_check(uri3, requester=users[3])
    assert simplify_perms(state['pendingPermissions']) == simplify_perms(EMPTY_PERMISSIONS), state['pendingPermissions']
    assert simplify_perms(state['permissions']) == simplify_perms(_expected), state['permissions']

    # Modify over deleted:
    req_and_check(
        uri3, method='patch', requester=users[0],
        json=modify_one_diff(
            perm_kind='acl_edit', user=users[3], new_data=dict(
                description='Promoting back', grantType='acl_adm')))
    _expected = dict(
        EMPTY_PERMISSIONS,
        acl_adm=[
            dict(INITIAL_INHERITED, name=users[0]),
            dict(name=users[3], description='Promoting back', approver=users[0], requester=users[0]),
        ],
        # # Pre-dedup:
        # acl_view=[dict(INITIAL_FOR_OWNER, name=users[3], description='Demoted permissions')],
        # # Dedup:
        acl_view=[],
    )
    state = req_and_check(uri3, requester=users[3])
    assert simplify_perms(state['pendingPermissions']) == simplify_perms(EMPTY_PERMISSIONS)
    assert simplify_perms(state['permissions']) == simplify_perms(_expected), state

    resp = req_and_check(
        'nodes/all/{}/permissions/subjects/{}'.format(uuids[3], users[3]), requester=users[0])
    assert resp['history'], resp

    # PUT permissions
    req_and_check(
        uri3, method='put', requester=users[0],
        json=dict(
            default_comment='full replace',
            permissions=dict(acl_adm=[
                dict(subject=users[0], comment='just me'),
            ])))
    _expected = dict(
        EMPTY_PERMISSIONS,
        acl_adm=[
            dict(name=users[0], description='just me', approver=users[0], requester=users[0]),
        ])
    state = req_and_check(uri3, requester=users[0])
    assert simplify_perms(state['pendingPermissions']) == simplify_perms(EMPTY_PERMISSIONS), state['pendingPermissions']
    assert simplify_perms(state['permissions']) == simplify_perms(_expected), state['permissions']

    # TODO: test acl_deny

    # initialPermissionsMode = 'explicit'
    uuid_ie = make_uuid()
    ie_permissions = dict(
        acl_adm=[dict(subject='system_group:all_active_users')],
        acl_edit=[dict(subject=users[3])],
        acl_view=[dict(subject=users[2], comment="Just for testing")],
    )
    nodeinfo_ie = req_and_check('nodes/entries/{}'.format(uuid_ie), method='put', json=dict(
        scope='whateverX',
        initialOwner=users[4],
        initialPermissionsMode='explicit',
        initialPermissions=ie_permissions,
    ))
    LOGGER.info("nodeinfo_ie: %r", nodeinfo_ie)

    _expected = dict(
        acl_adm=[dict(
            INITIAL_EXPLICIT_MARKER,
            name='system_group:all_active_users',
            approver=ROOT_USER,
            requester=ROOT_USER)],
        acl_edit=[dict(
            INITIAL_EXPLICIT_MARKER,
            name=users[3],
            approver=ROOT_USER,
            requester=ROOT_USER)],
        acl_view=[dict(
            INITIAL_EXPLICIT_MARKER,
            name=users[2],
            description="Just for testing",
            approver=ROOT_USER,
            requester=ROOT_USER)],
    )

    state = req_and_check('nodes/all/{}/permissions'.format(uuid_ie), requester=users[1])
    assert simplify_perms(state['permissions']) == simplify_perms(_expected), state['permissions']
    assert simplify_perms(state['pendingPermissions']) == dict(EMPTY_PERMISSIONS)

    # ####### deduplication #######

    subtest_dedup(users=users)

    # ####### search (suggest) #######
    # Not applicable for cloud tests.
    # subtest_suggest(users=users)

    if PERF_PERM_CHECK:
        subtest_perf_perm_check(uuids=uuids, users=users)


def main():
    try:
        from pyaux.runlib import init_logging
        init_logging(level=1)
    except Exception:
        pass

    return test_main()

if __name__ == '__main__':
    test_main()

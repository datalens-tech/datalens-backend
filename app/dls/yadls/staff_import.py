# coding: utf8
"""
...
"""


from __future__ import annotations

import os
import sys
import logging
import uuid
import time
import functools
from collections import defaultdict
from urllib.parse import urljoin, urlparse, parse_qs
from typing import Any, Callable

import requests
from requests.packages.urllib3.util import Retry  # pylint: disable=import-error
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg

from .utils import (
    LimitReached, filter_fields,
    make_uuid, ascii_to_uuid_prefix, prefixed_uuid,
    groupby, chunks,
    maybe_postmortem,
)
from .settings import settings
from . import db as db_base
from . import db_utils
from .manager_inmem import DLSInmem
from .manager_aiopg import DLSPGBase


LOG = logging.getLogger('staff_import')
DENORMALIZED_STRUCTURE = False
ANNOTATED = True


class StaffImporter:

    db_get_engine = staticmethod(db_base.get_engine)
    Grant = db_base.Grant
    Log = db_base.Log
    NodeConfig = db_base.NodeConfig
    Subject = db_base.Subject
    group_members_m2m = db_base.group_members_m2m
    ss_word_subjects_m2m = db_base.ss_word_subjects_m2m

    _staff_base_url = 'https://staff-api.yandex-team.ru/v3/'
    _idm_base_url = 'https://idm-api.yandex-team.ru/api/v1/'
    _abc_base_url = 'https://abc-back.yandex-team.ru/api/v3/'
    retry_conf = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504, 521],
        method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE', 'POST']),
    )
    db_engine = None
    db_conn = None
    _source = 'staff'
    insert_chunk_size = db_base.DEFAULT_INSERT_CHUNK_SIZE
    delete_chunk_size = db_base.DEFAULT_DELETE_CHUNK_SIZE

    def __init__(self):
        assert settings.STAFF_TOKEN
        self._token = settings.STAFF_TOKEN

        session = requests.Session()
        retry_conf = self.retry_conf
        for prefix in ('http://', 'https://'):
            session.mount(
                prefix,
                requests.adapters.HTTPAdapter(
                    max_retries=retry_conf,
                    pool_connections=30, pool_maxsize=30,
                ))
        self._reqr = session
        self._default_auth_headers = dict(Authorization="OAuth {}".format(self._token))

    def req(self, url, params, *, headers=None, method='get', _default_auth=True, **kwargs):
        headers = headers or {}
        if _default_auth:
            headers = {**self._default_auth_headers, **headers}
        kwargs.setdefault('timeout', 120)
        kwargs.setdefault('allow_redirects', False)
        resp = self._reqr.request(method, url, params=params, headers=headers, **kwargs)
        if resp.status_code not in (200, 201):
            raise requests.HTTPError(
                "Not an okay status: {}; {}".format(resp.status_code, resp.text),
                response=resp)
        return resp

    def req_staff(self, path, *args, **kwargs):
        url = urljoin(self._staff_base_url, path)
        return self.req(url, *args, **kwargs)

    def _iterate_staff_common(
            self, endpoint, fields, current_id=0, limit=10000, max_pages=5000,
            params=None):
        assert isinstance(fields, str)
        params = dict(params or {}, _sort='id', _limit=limit, _fields=fields, _pretty=0)
        for page in range(max_pages):
            items = self.req_staff(
                endpoint, params=dict(params, _query='id > {}'.format(current_id))
            ).json()['result']
            if not items:
                return
            for item in items:
                current_id = max(current_id, item['id'])
                yield item
        raise LimitReached(max_pages)

    # Notable:
    # department_group.id,emails.address,groups.id,language,name,official.quit_at,
    # robot_users.id,yandex.login,_meta.modified_at,
    _user_fields = (
        'id,uid,login,name,'
        'is_deleted,official.is_dismissed,official.quit_at,'
        'official.is_robot,official.is_homeworker,official.affiliation,'
        'created_at,'
        'chiefs.uid,'
    )

    def iterate_staff_users(self, **kwargs):
        kwargs.setdefault('fields', self._user_fields)
        return self._iterate_staff_common('persons', **kwargs)

    # Notable: service.id,department.id,department.url,ancestors.id,description,_meta.modified_at
    _group_fields = (
        'id,type,url,'
        # 'is_deleted,'
        'name,department.name,'
        'parent.id,parent.type,parent.url,parent.name,parent.department.name,'
        # 'ancestors.id,ancestors.name,ancestors.department.name,'
    )

    # `type`: department|service|servicerole|wiki
    def iterate_staff_groups(self, **kwargs):
        kwargs.setdefault('fields', self._group_fields)
        return self._iterate_staff_common('groups', **kwargs)

    # Notable: _meta.modified_at,joined_at
    # _groupmembership_fields = 'id,group.id,person.id,person.login'
    _groupmembership_fields = 'id,group.id,person.id'  # minimal

    def iterate_staff_groupmemberships(self, **kwargs):
        kwargs.setdefault('fields', self._groupmembership_fields)
        # TODO: add `group.is_deleted=false` by default.
        return self._iterate_staff_common('groupmembership', **kwargs)

    def req_idm(self, path, *args, **kwargs):
        url = urljoin(self._idm_base_url, path)
        return self.req(url, *args, **kwargs)

    # # A different interpretation of 'active': 'expected to be active'.
    # _idm_default_states = (
    #     'requested,imported,created,granted,rerequested,failed,need_request,'
    #     'onhold,review_request,approved,sent'
    # )
    _idm_default_states = (
        'rerequested,need_request,granted,depriving,review_request'
    )

    def iterate_idm_roles(
            self, system=None, role_path=None, state=_idm_default_states,
            parent_type='absent', fields=None, limit=5000, offset=0,
            max_pages=5000, **kwargs):

        if fields is not None:
            if isinstance(fields, str):
                fields = fields.split(',')

        # TODO: https://st.yandex-team.ru/IDM-5658
        # TODO: https://st.yandex-team.ru/IDM-1513
        params = dict(
            kwargs, system=system, path=role_path, state=state, order_by='id', format='json',
            limit=limit, offset=offset)
        if parent_type is not None:
            params['parent_type'] = parent_type
        if state is not None:
            assert isinstance(state, str)
            params['state'] = state

        for page in range(max_pages):
            data = self.req_idm('roles/', params=params).json()
            for item in data['objects']:
                if fields is not None:
                    item = filter_fields(item, fields=fields)
                yield item
            if not data['meta']['next']:
                return
            next_params = parse_qs(urlparse(data['meta']['next']).query)
            prev_params = dict(params)
            params.update(next_params)
            assert params != prev_params
        raise LimitReached(max_pages)

    # Notable: added,node.state,node.slug,parent,
    _idm_group_members_fields = (
        'id,data.group,data.role,'
        'group.id,group.slug,'
        'is_active,state,updated,'
        'user.username,'
    )

    def iterate_idm_group_members(
            self, group_id, kind=None, role='member',
            fields=_idm_group_members_fields, **kwargs):
        if group_id == '*all*':  # special value to avoid making it the default
            role_path = '/groups/'
        else:
            role_path = '/groups/{}/'.format(group_id)
        if kind is not None:
            assert kind in ('member', 'responsible')
            role_path = '{}{}/'.format(role_path, kind)
        result = self.iterate_idm_roles(
            system='staff', role_path=role_path, fields=fields, **kwargs)
        if role is not None:
            result = (item for item in result if item['data']['role'] == role)
        return result

    def req_abc(self, path, *args, **kwargs):
        url = urljoin(self._abc_base_url, path)
        return self.req(url, *args, **kwargs)

    def _iterate_abc_common_straight(self, endpoint, max_pages=5000, params=None, fields=None):
        params = dict(params or {})

        if fields is not None:
            if isinstance(fields, str):
                fields = fields.split(',')

        for page in range(max_pages):
            data = self.req_abc(endpoint, params=params).json()
            for item in data['results']:
                if fields is not None:
                    item = filter_fields(item, fields=fields)
                yield item
            if not data['next']:
                return
            next_params = parse_qs(urlparse(data['next']).query)
            prev_params = dict(params)
            params.update(next_params)
            assert params != prev_params

        raise LimitReached(max_pages)

    def _iterate_abc_common_threaded(
            self, endpoint, max_pages=5000, params=None, fields=None,
            pool_size=10):
        params = dict(params or {})

        if fields is not None:
            if isinstance(fields, str):
                fields = fields.split(',')

        def req_one(page):
            try:
                data = self.req_abc(endpoint, params=dict(params, page=page)).json()
            except requests.HTTPError as exc:
                resp = exc.response
                if (exc.response.status_code == 404 and
                        resp.json()['error']['detail'] in (
                            'Invalid page.', 'Неправильная страница')):
                    return dict(page=page, results=None, invalid_page=True)
                raise

            results = data['results']
            if fields is not None:
                results = [filter_fields(item, fields=fields) for item in results]
            return dict(page=page, results=results)

        # total_pages = self.req_abc(endpoint, params=params).json()['total_pages']
        import multiprocessing.pool
        from queue import Queue
        pool = multiprocessing.pool.ThreadPool(pool_size)
        queue: Queue[multiprocessing.pool.ApplyResult] = Queue()

        def add_job(page):
            if page > max_pages:
                raise LimitReached(page)
            job = pool.apply_async(req_one, (page,))
            queue.put(job)
            return job

        for page in range(1, pool_size + 1):
            add_job(page)

        while not queue.empty():  # unprocessed results await
            job = queue.get()
            job_result = job.get()
            if job_result.get('invalid_page'):
                continue  # dead end
            add_job(job_result['page'] + pool_size)
            for item in job_result['results']:
                yield item

    # _iterate_abc_common = _iterate_abc_common_straight
    _iterate_abc_common = _iterate_abc_common_threaded

    # Notable: modified_at,owner.id,slug
    _abc_services_fields = 'id,slug,parent.id'

    def iterate_abc_services(self, fields=None, **kwargs):
        return self._iterate_abc_common(
            'services/',
            fields=fields or self._abc_services_fields,
            **kwargs)

    # Notable: created_at,modified_at,state
    _abc_memberships_fields = (
        'id,state,'
        'person.id,person.login,'
        'service.id,service.slug,'
        'role.scope.slug,'
    )

    def iterate_abc_memberships(self, fields=None, params=None, **kwargs):
        if params is None:
            params = dict(state='approved')
        return self._iterate_abc_common(
            'services/members/',
            fields=fields or self._abc_memberships_fields,
            **kwargs)

    def _all_datas(self, extended=DENORMALIZED_STRUCTURE, cached=None):
        # ~2min
        items_gens: tuple[tuple[str, Callable], ...] = (
            ('persons', self.iterate_staff_users),
            ('groups', self.iterate_staff_groups),
            ('groupmemberships', self.iterate_staff_groupmemberships),
        )
        if extended:
            items_gens += (
                # ~1min
                ('idm_group_members',
                 functools.partial(self.iterate_idm_group_members, group_id='*all*')),
                # ~0.5min
                ('abc_services', self.iterate_abc_services),
                # ~10min
                ('abc_memberships', self.iterate_abc_memberships),
            )

        items = tuple((key, func()) for key, func in items_gens)

        # The above structure does no calls, and can be used as a list of the expected items.
        if cached is None:
            cached = bool(os.environ.get('DLS_STAFF_IMPORT_FORCE_CACHED'))
        if cached:
            import glob
            import msgpack
            flns = glob.glob('_tmp_*.msgp')
            data = {fln[5:-5]: msgpack.Unpacker(open(fln, "rb")) for fln in flns}
            try:
                cached_items = tuple((key, data[key]) for key, _ in items)
            except KeyError as exc:
                LOG.warning("Not everything required is present in the cache: %r", exc)
            else:
                items = cached_items

        return items

    def dump_cache(self, extended=DENORMALIZED_STRUCTURE, parallel=True):
        import msgpack
        from atomicwrites import AtomicWriter
        items = self._all_datas(extended=extended, cached=False)

        def dump_one(fln, gen):
            t1 = time.time()
            packer = msgpack.Packer(use_bin_type=True)
            with AtomicWriter('_tmp_{}.msgp'.format(fln), 'wb', overwrite=True).open() as fo:
                for item in gen:
                    fo.write(packer.pack(item))
            t2 = time.time()
            LOG.info("Dumping %r: %.3fs", fln, t2 - t1)

        if parallel:
            from multiprocessing.pool import ThreadPool
            pool = ThreadPool(10)
            jobs = [pool.apply_async(dump_one, (fln, gen)) for fln, gen in items]
            for job in jobs:
                job.get()
        else:
            for fln, gen in items:
                dump_one(fln, gen)

        LOG.info("Done dumping.")

    @classmethod
    def _process_user_meta(cls, item, annotated=ANNOTATED):
        """ mangle around to some common expected format """
        result = {}
        result['__uid'] = item.get('uid')
        login = item.get('login', None)
        result['__login'] = login
        result['__rlsid'] = login
        name = item.get('name', None) or login
        if name and isinstance(name, dict):
            name = {
                lang: '{} {}'.format(
                    name.get('first', {}).get(lang, ''),
                    name.get('last', {}).get(lang, ''),
                )
                for lang in ('ru', 'en')}

        result['url_data'] = login
        result['title'] = name  # display text data
        if annotated:
            result['_staff_item'] = item
        return result

    @classmethod
    def _process_group_meta(cls, item, annotated=ANNOTATED):
        """ mangle around to some common expected format """
        result = {}
        url = item.get('url')

        if url:
            result['__rlsid'] = 'group:{}'.format(url)

        # For both searching and the actual URL, the abc services need to be
        # without the 'svc_' prefix:
        if url and item.get('type') in ('service', 'servicerole'):
            prefix = 'svc_'
            if url.startswith(prefix):
                url = url[len(prefix):]

        try:
            name = item['department']['name']['full']
        except (TypeError, KeyError):
            name = item.get('name') or url

        result['url_data'] = url
        result['title'] = name  # display text data
        result['type'] = item.get('type')  # source-specific type

        parent = item.get('parent')
        if parent:
            result['parent'] = cls._process_group_meta(parent, annotated=False)

        if annotated:
            result['_staff_item'] = item
        return result

    @classmethod
    def process_staff_data(
            cls,
            groups, persons, groupmemberships,
            idm_group_members=None, abc_services=None, abc_memberships=None,
            annotated=ANNOTATED, filtered=True, skip_denormalized=True,
    ):
        groups = list(groups)
        assert groups
        persons = list(persons)
        assert persons
        groupmemberships = list(groupmemberships)
        assert groupmemberships
        if idm_group_members is not None:
            idm_group_members = list(idm_group_members)
        if abc_services is not None:
            abc_services = list(abc_services)
        if abc_memberships is not None:
            abc_memberships = list(abc_memberships)

        if filtered:
            persons = list(
                item for item in persons
                if not item['is_deleted'] and
                not item['official']['is_dismissed']
            )

        users_by_login = None

        groups_by_id = {item['id']: item for item in groups}
        users_by_id = {item['id']: item for item in persons}

        # NOTE: subject is an `uuid` rather than `staff id` to avoid user/group ambiguity.
        # `staff_group_id` is an `staff id` for simplicity, because it is always a group.
        # {(staff_group_id, subject_uuid) -> [SubjectGrant, ...]}
        memberships = defaultdict(list)

        for item in groups:
            parent_id = item.get('parent', {}).get('id')
            if not parent_id:
                continue

            subject = dict(id=group_id_to_uuid(item['id']))
            if annotated:
                subject.update(meta=item, reason='staff_child_group')

            memberships[(parent_id, subject['id'])].append(subject)

        # # No memberships to dig out of there:
        # for item in persons:
        #     subject = dict(id=user_id_to_uuid(item['id']))
        #     if annotated:
        #         subject.update(meta='item', reason='staff_person_info')

        # Expecting:
        #  * Direct 'department' memberships.
        #  * Recursive 'service' memberships. (optional)
        #  * Recursive 'servicerole' memberships. (optional)
        #  * Recursive 'wiki' memberships. (optional)
        if skip_denormalized == 'auto':
            skip_denormalized = idm_group_members and abc_services and abc_memberships
        elif skip_denormalized:
            assert idm_group_members and abc_services and abc_memberships

        for item in groupmemberships:
            if item['group']['id'] not in groups_by_id:
                continue
            if item['person']['id'] not in users_by_id:
                continue

            if skip_denormalized:
                group_info = groups_by_id[item['group']['id']]
                if group_info['type'] != 'department':
                    continue

            subject = dict(id=user_id_to_uuid(item['person']['id']))
            if annotated:
                subject.update(meta=item, reason='staff_groupmembership')

            memberships[(item['group']['id'], subject['id'])].append(subject)

        # For comparison and debug, also support the group-group memberships for wiki and abc:

        if idm_group_members:
            users_by_login = {item['login']: item for item in persons}

            for item in idm_group_members:
                # # Apparently, IDM only recurses over 'member' roles, but not 'responsible' roles.
                # assert item['data']['role'] == 'member'
                if item['data']['role'] != 'member':
                    continue

                group_id = int(item['data']['group'])
                if item.get('group'):
                    subject = dict(id=group_id_to_uuid(item['group']['id']))
                else:
                    user_login = item['user']['username']
                    user_info = users_by_login.get(user_login)
                    if user_info is None:
                        continue
                    user_id = user_info['id']
                    subject = dict(id=user_id_to_uuid(user_id))

                if annotated:
                    subject.update(meta=item, reason='idm_group_member')
                memberships[(group_id, subject['id'])].append(subject)

        if abc_services or abc_memberships:
            groups_by_url = {(item['type'], item['url']): item for item in groups}
            abc_service_by_abc_id = {item['id']: item for item in abc_services}

            def abc_id_to_staff_id(abc_id):
                svc = abc_service_by_abc_id.get(abc_id)
                if svc is None:
                    return None
                staff_item = groups_by_url.get(('service', 'svc_{}'.format(svc['slug'].lower())))
                if staff_item is None:
                    return None
                return staff_item['id']

        if abc_services:

            for item in abc_services:
                if not item.get('parent'):
                    continue

                staff_id = abc_id_to_staff_id(item['id'])
                if staff_id is None:  # e.g. a deleted group
                    continue

                subject = dict(id=group_id_to_uuid(staff_id))
                if annotated:
                    subject.update(meta=item, reason='abc_subgroup')

                parent_staff_id = abc_id_to_staff_id(item['parent']['id'])
                if parent_staff_id:
                    memberships[(parent_staff_id, subject['id'])].append(subject)

        if abc_memberships:

            if skip_denormalized:
                # `servicerole` memberships are implied from abc_memberships
                groups_by_url = {(item['type'], item['url']): item for item in groups}
                service_to_roles: dict[str, dict] = defaultdict(dict)

            # users_by_login = {item['login']: item for item in persons}
            for item in abc_memberships:
                svc_slug = item['service']['slug'].lower()
                role_slug = item['role']['scope']['slug']

                if item['person']['id'] not in users_by_id:  # invalid user
                    continue

                service_staff_id = abc_id_to_staff_id(item['service']['id'])
                if service_staff_id is None:  # invalid group, somehow
                    continue

                if users_by_login is not None:
                    # Check that ABC still returns a staff user id.
                    assert users_by_login[item['person']['login']]['id'] == item['person']['id']

                subject = dict(id=user_id_to_uuid(item['person']['id']))
                if annotated:
                    subject.update(meta=item, reason='abc_membership')
                memberships[(service_staff_id, subject['id'])].append(subject)

                if skip_denormalized:
                    # Make the servicerole membership
                    servicerole = 'svc_{}_{}'.format(svc_slug, role_slug)
                    sr_staff_group = groups_by_url.get(('servicerole', servicerole))
                    if sr_staff_group:
                        m_key = (sr_staff_group['id'], subject['id'])
                        memberships[m_key].append(
                            dict(subject, reason='abc_membership_servicerole'))

                        # Ensure the service -> servicerole dependency
                        sr_subject = dict(id=group_id_to_uuid(sr_staff_group['id']))
                        if annotated:  # `meta` here serves only as an example.
                            sr_subject.update(meta=item, reason='abc_service_role')
                        memberships[(service_staff_id, sr_subject['id'])] = [sr_subject]
                        service_to_roles[svc_slug][role_slug] = dict(
                            servicerole=servicerole,
                            role_scope_slug=role_slug,
                            service_staff_id=service_staff_id,
                            sr_staff_id=sr_staff_group['id'])

        if skip_denormalized:
            # Tricky point: servicerole dependencies
            # Example:
            #   svc_infrastatistics -> svc_statinfrafrontend,
            #   therefore,
            #   svc_infrastatistics_development -> svc_statinfrafrontend_development
            # (see `_dbg` "Tests" for an actual example)
            assert service_to_roles

            abc_id_to_abc_slug = {item['id']: item['slug'] for item in abc_services}

            svc_links = []
            svc_roots = []
            for item in abc_services:
                if not item.get('parent'):
                    svc_roots.append(item['slug'])
                    continue
                parent_slug = abc_id_to_abc_slug.get(item['parent']['id'])
                if parent_slug is not None:
                    # The parent service might be 'not exported'
                    # https://wiki.yandex-team.ru/intranet/abc/vodstvo/create/#other
                    # Last known example:
                    # https://abc.yandex-team.ru/services/edadeal-redis-cluster-dev/
                    svc_links.append((parent_slug, item['slug']))

            svc_to_subservices = groupby(svc_links)

            def walk(slug, ancestors=()):
                svc_roles = service_to_roles.get(slug, {})

                for role_slug, info in svc_roles.items():
                    srr_subject = dict(id=group_id_to_uuid(info['sr_staff_id']))
                    if annotated:
                        srr_subject.update(meta=info, reason='abc_servicerole_of_a_subgroup')
                    # Chain-add each service-role as a member of an ancestor's service-role
                    for ancestor_slug in reversed(ancestors):
                        ancestor_info = service_to_roles.get(ancestor_slug, {}).get(role_slug)
                        if ancestor_info:
                            m_key = (ancestor_info['sr_staff_id'], srr_subject['id'])
                            memberships[m_key] = [srr_subject]
                            break

                # Recurse over the service's children:
                children = svc_to_subservices.get(slug)
                if children:
                    new_ancestors = ancestors + (slug,)
                    for child in children:
                        walk(child, ancestors=new_ancestors)

            for svc_root_slug in svc_roots:
                walk(svc_root_slug)

        result: dict[str, Any] = {}

        result['users'] = list(
            dict(
                id=user_id_to_uuid(item['id']),
                name='user:{}'.format(item['uid']),
                path=('.users', '.staff', item['login']),
                active=not item['official']['is_dismissed'],
                meta=cls._process_user_meta(item, annotated=annotated),
            )
            for item in persons
        )

        result['groups'] = list(
            dict(
                id=group_id_to_uuid(item['id']),
                name='group:{}'.format(item['id']),
                path=('.groups', '.staff', item['type'], item['url']),
                active=True,
                meta=cls._process_group_meta(item, annotated=annotated),
            )
            for item in groups)

        group_id_to_subjects = groupby(
            (group_id, subject)
            for (group_id, subject_id), subjects in memberships.items()
            for subject in subjects)

        result['group_to_subjects'] = {
            group_id_to_uuid(group_id): subjects
            for group_id, subjects in group_id_to_subjects.items()
            if group_id in groups_by_id  # skip e.g. deleted groups
        }

        if annotated:
            result['_memberships'] = memberships
            result['_all'] = locals()

        return result

    def dbg_build(self, extended=DENORMALIZED_STRUCTURE, annotated=ANNOTATED, cached=None, dump_dot=True):
        data = dict(self._all_datas(extended=extended, cached=cached))
        data = {key: list(value) for key, value in data.items()}

        # pylint: disable=possibly-unused-variable
        groups_by_id = {item['id']: item for item in data['groups']}
        # pylint: disable=possibly-unused-variable
        groups_by_url = {(item['type'], item['url']): item for item in data['groups']}
        # pylint: disable=possibly-unused-variable
        users_by_login = {item['login']: item for item in data['persons']}
        # pylint: disable=possibly-unused-variable
        users_by_id = {item['id']: item for item in data['persons']}
        # pylint: disable=possibly-unused-variable
        g2m_idm = groupby((int(item['data']['group']), item) for item in data['idm_group_members'])
        # pylint: disable=possibly-unused-variable
        g2m_dpt = groupby((item.get('parent', {}).get('id'), item) for item in data['groups'])
        # pylint: disable=possibly-unused-variable
        g2m_staff = groupby((item['group']['id'], item) for item in data['groupmemberships'])
        _stats = {key: len(val) for key, val in locals().items() if isinstance(val, dict) and not key.startswith('_')}
        print(_stats)

        result = self.process_staff_data(
            groups=data['groups'],
            persons=data['persons'],
            groupmemberships=data['groupmemberships'],
            # Optional:
            idm_group_members=data.get('idm_group_members'),
            abc_services=data.get('abc_services'),
            abc_memberships=data.get('abc_memberships'),

            # ...
            annotated=annotated,
        )

        # Tests:
        #
        # https://idm.yandex-team.ru/system/stat-beta/roles#f-is-expanded=true,f-role=stat-beta/statreport/84985,f-status=all,sort-by=-updated,f-user=robot-stat-jam,f-user=slava,f-group=67526
        # abc 'infrastatistics' -> robot-stat-jam@ (belongs to statinfrafrontend)
        #
        # https://idm.yandex-team.ru/system/stat-beta/roles#f-is-expanded=true,f-role=stat-beta/statreport/84984,f-status=all,sort-by=-updated,f-user=slava,f-user=roman-ko,f-user=remean,f-user=asnytin,f-group=91
        # dpt 'yandex_infra_tech_stat' -> slava@, roman-ko@, remean@, asnytin@
        #
        # https://idm.yandex-team.ru/system/stat-beta/roles#f-is-expanded=true,f-role=stat-beta/statreport/84986,f-status=all,sort-by=-updated,f-user=chertkova,f-user=robot-statbox-theta,f-user=dalamar,f-user=asnytin,f-group=81096,f-group=79620
        # https://idm.yandex-team.ru/system/staff/roles#f-status=active,f-role=staff/groups/81096,sort-by=subject,f-is-expanded=true,f-user=asnytin,f-user=chertkova,f-user=dalamar,f-user=robot-statbox-theta,f-group=79620
        # wiki stat_devs -> robot-statbox-theta@, asnytin@
        #
        # https://idm.yandex-team.ru/system/stat-beta/roles#f-is-expanded=true,f-role=stat-beta/statreport/84987,f-status=all,sort-by=-updated,f-user=resure,f-group=29420
        # servicerole svc_qc_development -> resure@, not kender@
        # servicerole svc_infrastatistics_virtual -> robot-statbox@, robot-stat-jam@

        # ...
        mgr = DLSInmem()

        for item in result['users']:
            item.pop('active', None)
            mgr._add_user(**item)

        for item in result['groups']:
            mgr.add_group(**item)

        for group_uuid, subjects in result['group_to_subjects'].items():
            subjects_uuids = list(set(subject['id'] for subject in subjects))
            mgr.set_group_subjects(group=group_uuid, subjects=subjects_uuids)

        mgr._ensure_effective_groups()

        # pylint: disable=possibly-unused-variable
        memberships = list(
            dict(item, grp=key)
            for key, lst in result['group_to_subjects'].items()
            for item in lst)

        if dump_dot:
            graph_g = (
                (grp.path, subj.path)
                for grp in mgr.store['groups']
                for subj in grp.get_subjects())
            path2str = lambda path: "/".join(path[:1] + path[2:])
            with open('memberships.dot', 'w') as fo:
                fo.write('digraph memberships {\n')
                for grp, subj in graph_g:
                    fo.write('  "{}" -> "{}";\n'.format(path2str(grp), path2str(subj)))
                fo.write('}\n')

        def _subjs(lst):
            return [mgr._get_subject_node(item['id']) for item in lst]

        return locals()

    def main(
            self, from_cached=None,
            extended=DENORMALIZED_STRUCTURE, skip_denormalized=DENORMALIZED_STRUCTURE,
            annotated=ANNOTATED,
            filtered=False, source=None, use_pgcopy=False, memberships_by_diff=True,
            delete_obsolete_subjects=False,
            run_cache_manager=True, run_subjects_manager=True,
    ):
        NodeConfig = self.NodeConfig
        Subject = self.Subject
        m2m = self.group_members_m2m
        Grant = self.Grant
        m2m_words = self.ss_word_subjects_m2m

        if source is None:
            source = self._source

        LOG.info("Obtaining source data (extended=%r, cached=%r).", extended, from_cached)
        datas = self._all_datas(extended=extended, cached=from_cached)

        LOG.info("Prebuilding structure (filtered=%r, annotated=%r, skip_denormalized=%r).",
                 filtered, annotated, skip_denormalized)
        structure = self.process_staff_data(
            **dict(datas),
            **dict(
                filtered=filtered,
                annotated=annotated,
                skip_denormalized=skip_denormalized))

        LOG.info("...")
        mgr = DLSPGBase()

        # Add the 'system_group:all_active_users'
        structure['group_to_subjects'][mgr.ACTIVE_USER_GROUP_UUID] = list(
            user for user in structure['users'] if user['active'])

        all_new_subjects = set(item['id'] for item in structure['users'] + structure['groups'])

        LOG.info("Databasing...")
        self.db_engine = self.db_get_engine()
        with self.db_engine.begin() as db_conn:
            self.db_conn = db_conn

            subj_uuid_to_id = {}  # uuid -> id

            LOG.info("Databasing: system groups.")
            system_groups_data = mgr.get_system_groups_data()
            staff_statleg_uuid = prefixed_uuid('s_g_statleg')
            system_groups_data = system_groups_data + (
                dict(
                    kind='group', source='system',
                    name='system_group:staff_statleg', uuid=staff_statleg_uuid,
                    search_weight=-1000,
                    meta=dict(
                        name='s_g_staff_statleg',
                        title=dict(ru='Все (без outstaff)', en='All (except outstaff)'),
                    )),
            )
            system_groups = self._upsert_subjects(system_groups_data, source='system')
            subj_uuid_to_id.update(system_groups)

            LOG.info("Databasing: subjects obsoleties.")
            stmt = (
                Subject.__table__.select().join(Subject.node_config)
                .where(Subject.source == source)
                .with_only_columns([Subject.id, NodeConfig.node_identifier])
            )
            # Join in-mem; another way would be to upload the all_new_subjects
            # to a temp table on the psql server and join in there.
            subjects_pre = list(db_conn.execute(stmt))
            former_subjects = list(
                row for row in subjects_pre
                if row.node_identifier not in all_new_subjects)
            LOG.info("Databasing: subjects obsoleties: %r; deleting=%r", len(former_subjects), delete_obsolete_subjects)
            if former_subjects:
                former_subjects_ids = set(item.id for item in former_subjects)
                if delete_obsolete_subjects:
                    db_conn.execute(m2m_words.delete_(
                        m2m_words.subject_id.in_(former_subjects_ids)))

                    Log = self.Log
                    grants_filter = Grant.subject_id.in_(former_subjects_ids)
                    grant_guid_sq = Grant.select_(grants_filter).with_only_columns([Grant.guid])
                    db_conn.execute(Log.delete_(Log.grant_guid.in_(grant_guid_sq)))
                    db_conn.execute(Grant.delete_(grants_filter))

                    db_conn.execute(m2m.delete_(
                        (m2m.group_id.in_(former_subjects_ids)) |
                        (m2m.member_id.in_(former_subjects_ids))
                    ))

                    db_conn.execute(Subject.delete_(
                        Subject.id.in_(former_subjects_ids)))

                    former_subjects_nids = set(item.node_identifier for item in former_subjects)
                    db_conn.execute(NodeConfig.delete_(
                        NodeConfig.node_identifier.in_(former_subjects_nids)))

                    # former_subjects_ncids = set(item.node_config_id for item in former_subjects)
                    # db_conn.execute(NodeConfig.delete_(NodeConfig.id.in_(former_subjects_ncids)))
                else:
                    stmt = (
                        Subject.__table__.update()
                        .where(Subject.id.in_(former_subjects_ids))
                        .values(active=False))
                    db_conn.execute(stmt)

            # Additionally fill the mapping so that we don't have to return id
            # from upsert over the existing rows.
            subj_uuid_to_id.update((row.node_identifier, row.id) for row in subjects_pre)

            # XXXX/TODO: do not touch the items that weren't significantly
            # changed (on conflict ignore + inmem compare)

            def _make_search_weight(item):
                weight = 0
                if item['meta'].get('type') == 'wiki':
                    weight -= 100
                # if item['meta'].get('_staff_item', {}).get('id') == 962:
                if item['name'] == 'group:962':
                    weight += 320
                return weight

            LOG.info("Databasing: subjects.")
            # TODO/PERF: do not upsert unchanged items.
            subjectses = (
                ('group', structure['groups']),
                ('user', structure['users']),
            )
            current_subjects = list(
                dict(
                    kind=kind,
                    name=item['name'],
                    active=item.get('active', True),
                    search_weight=_make_search_weight(item),
                    uuid=item['id'],
                    meta=item['meta'],
                )
                for kind, subjects in subjectses
                for item in subjects)

            LOG.info("Databasing: subjects: %r", len(current_subjects))
            subjects_res = self._upsert_subjects(
                current_subjects, source=source, return_all=False)
            subj_uuid_to_id.update(subjects_res)

            LOG.info("Databasing: memberships: prebuild.")
            memberships = list(
                dict(
                    group_id=subj_uuid_to_id[group_uuid],
                    member_id=subj_uuid_to_id[subj_uuid],
                    source=source,
                )
                for group_uuid, group_subjects in structure['group_to_subjects'].items()
                for subj_uuid in set(subject['id'] for subject in group_subjects)
            )

            # `system_group:staff_statleg` members:
            # ['group:962', 'group:51', 'group:8174', 'group:5']
            # https://staff-api.yandex-team.ru/v3/groups/?url=yandex,yandex_money,virtual,ext
            memberships += list(
                dict(
                    group_id=subj_uuid_to_id[staff_statleg_uuid],
                    member_id=subj_uuid_to_id[group_id_to_uuid(gid)],
                    source='system',
                )
                for gid in (962, 51, 8174, 5)
            )

            # robot-datalens superuser
            # https://staff-api.yandex-team.ru/v3/persons?login=robot-datalens
            for user_id in (37444, 9557):
                memberships.append(dict(
                    group_id=subj_uuid_to_id[mgr.SUPERUSER_GROUP_UUID],
                    member_id=subj_uuid_to_id[user_id_to_uuid(user_id)],
                    source='system',
                ))

            if memberships_by_diff:
                LOG.info("Databasing: memberships obsoleties.")
                memberships_existing = db_conn.execute(
                    m2m.select_(m2m.source == source)
                    .with_only_columns([m2m.group_id, m2m.member_id])
                )
                memberships_existing = set(
                    (row.group_id, row.member_id) for row in memberships_existing)

                # join in-mem:
                memberships_target = {
                    (item['group_id'], item['member_id']): item
                    for item in memberships}
                m_obsoleties = list(
                    item for item in memberships_existing
                    if item not in memberships_target)

                # Only attempt to add new ones (nothing to `update`):
                memberships = list(
                    value for key, value in memberships_target.items()
                    if key not in memberships_existing)

                LOG.info("Databasing: memberships obsoleties: %r", len(m_obsoleties))
                if m_obsoleties:
                    m_pk = sa.sql.expression.tuple_(m2m.group_id, m2m.member_id)
                    for chunk in chunks(m_obsoleties, self.delete_chunk_size):
                        where = m_pk.in_(chunk)
                        db_conn.execute(m2m.delete_(where))

            else:
                # Refill the source-specific memberships completely.
                LOG.info("Databasing: memberships: cleanup.")
                db_conn.execute(m2m.delete_(m2m.source == source))

            LOG.info("Databasing: memberships: insert %r", len(memberships))
            if memberships and use_pgcopy:
                data_tuples = list(
                    (item['group_id'], item['member_id'], item['source'])
                    for item in memberships)
                data_columns = ('group_id', 'member_id', 'source')
                self._pgcopy_into(
                    data=data_tuples, table=m2m.__table__, conn=db_conn,
                    columns=data_columns)
            elif memberships:
                stmt = sa_pg.insert(m2m).values(memberships).on_conflict_do_nothing()
                db_conn.execute(stmt)

            LOG.info("Databasing: done.")

        if run_cache_manager:
            LOG.info("Running EffectiveGroupsCacheManager")
            from .cache_manager import EffectiveGroupsCacheManager
            cmgr = EffectiveGroupsCacheManager()
            cmgr.db_engine = self.db_engine
            user_items = list(item for item in current_subjects if item['kind'] == 'user')
            users_names = list(item['name'] for item in user_items if item['active'])
            obsolete_users_names = list(item['name'] for item in user_items if not item['active'])
            cmgr.main(subjects_names=users_names, obsolete_subjects_names=obsolete_users_names)

        if run_subjects_manager:
            LOG.info("Running subjects manager")
            from .subjects_manager import SubjectsManager
            smgr = SubjectsManager()
            smgr.db_engine = self.db_engine
            smgr.main()

        LOG.info("Done.")

    def _upsert_objs_straight(self, table, items, key=('uuid',), id_col='id', **kwargs):
        return db_utils.common_upsert_objects_straight(
            table=table, items=items, db_conn=self.db_conn, key=key, id_col=id_col, **kwargs)

    def _upsert_objs(self, table, items, key=('uuid',), id_col='id', return_all=True, **kwargs):
        return db_utils.common_upsert_objects_twopart(
            table=table, items=items, db_conn=self.db_conn, key=key,
            id_col=id_col, return_all=return_all, **kwargs)

    def _upsert_subjects(
            self, subjects, source=None, return_by_uuid=True, return_all=True,
            **kwargs):
        """
        :param subjects: [{kind, name, uuid, ...}, ...]
        """
        if source is None:
            source = self._source

        subjects = list(
            dict(item, uuid=item.get('uuid') or make_uuid())
            for item in subjects)

        configs = list(
            dict(node_identifier=item['uuid'], meta=item.get('cfg_meta') or {})
            for item in subjects)
        configs_ids = {}

        for config_chunk in chunks(configs, self.insert_chunk_size):
            config_ids_chunk = self._upsert_objs(  # uuid -> id
                table=self.NodeConfig,
                items=config_chunk,
                key=('node_identifier',),
                id_col='id',
                columns=('meta',),
            )
            configs_ids.update(config_ids_chunk)

        subject_items = list(
            dict(
                kind=item['kind'],
                name=item['name'],
                active=item.get('active', True),
                node_config_id=configs_ids[item['uuid']],
                search_weight=item.get('search_weight', 0),
                source=source,
                meta=item.get('meta') or {},
            )
            for item in subjects)
        result = {}
        for chunk in chunks(subject_items, self.insert_chunk_size):
            chunk_result = self._upsert_objs(
                table=self.Subject,
                items=chunk,
                key=('name',),  # NOTE.
                columns=('kind', 'active', 'node_config_id', 'source', 'meta', 'search_weight'),
                return_all=return_all,
                **kwargs)
            result.update(chunk_result)
        if return_by_uuid:
            name_to_uuid = {item['name']: item['uuid'] for item in subjects}
            result = {name_to_uuid[key]: value for key, value in result.items()}
        return result

    # TODO: upsert using `copy` and temporary tables:
    # https://stackoverflow.com/a/13949654

    @staticmethod
    def _pgcopy_into(data, table, conn, columns, extra_sql=None, schema='public'):
        from io import BytesIO
        import pgcopy  # https://github.com/altaurog/pgcopy
        import pgcopy.copy
        import postgres_copy  # https://github.com/jmcarp/sqlalchemy-postgres-copy

        # MONKEYHAX:
        # (need some other support implementation for enums and text)
        def text_formatter(tmod, val):
            if isinstance(val, str):
                val = val.encode('utf-8')
            return pgcopy.copy.str_formatter(tmod, val)

        pgcopy.copy.type_formatters.update(
            dls_import_source=text_formatter,
        )

        table = getattr(table, '__table__', table)
        table_name = table.name
        full_table_name = '{}.{}'.format(schema, table_name)

        # # Not quite:
        # postgres_copy.copy_to(source=data, dest=table, engine_or_conn=conn)

        db_conn = conn
        conn, autoclose = postgres_copy.raw_connection_from(conn)
        try:
            # Not quite:
            # https://github.com/jmcarp/sqlalchemy-postgres-copy/blob/01ef522e8e46a6961e227069d465b0cb93e42383/postgres_copy/__init__.py#L68

            # mgr = pgcopy.CopyManager(conn=conn, table=full_table_name, cols=columns)
            # mgr.copy(data, BytesIO)

            # Use pgcopy to make the postgresql-binary-copy bytestream and then
            # manually `copy_expert`:

            # NOTE: it uses `table_name` to get the actual table schema.
            mgr = pgcopy.CopyManager(conn=conn, table=full_table_name, cols=columns)

            # NOTE: can use `tempfile.TemporaryFile` if the data doesn't fit in the memory.
            # NOTE: see also `mgr.threading_copy` that does the actual streaming by
            # using a thread and pipes.
            # TODO: an async streaming implementation using
            # `asyncpg.connection.Connection.copy_to_table`
            # (`asyncpg.connection.Connection._copy_in`)
            datastream = BytesIO()
            mgr.writestream(data, datastream)
            datastream.seek(0)

            # ... there should've been a better way ...
            quote_identifier = db_conn.dialect.preparer(db_conn.dialect).quote_identifier
            # def quote_identifier(string):
            #     return '"{}"'.format(string.replace('"', '""'))

            sql = 'COPY {schema}.{table} ({columns}) FROM STDIN WITH BINARY {extra}'.format(
                schema=quote_identifier(schema), table=quote_identifier(table_name),
                columns=', '.join(quote_identifier(col) for col in columns),
                extra=extra_sql or '')
            with conn.cursor() as cursor:
                # http://initd.org/psycopg/docs/cursor.html#cursor.copy_expert
                cursor.copy_expert(sql, datastream)
        finally:
            if autoclose:
                conn.commit()  # ?
                conn.close()


Worker = StaffImporter

UUID_HEX_LEN = 32
STAFF_GROUP_PFX = ascii_to_uuid_prefix('staff_g')  # '73746166-665f-67..-...'
STAFF_USER_PFX = ascii_to_uuid_prefix('staff_u')  # '73746166-665f-75..-...'


def group_id_to_uuid(group_id):
    return str(uuid.UUID(int=STAFF_GROUP_PFX + group_id))


def user_id_to_uuid(user_id):
    return str(uuid.UUID(int=STAFF_USER_PFX + user_id))


def main(meth=None):
    if meth is None:
        try:
            meth = sys.argv[1]
        except IndexError:
            meth = 'main'
    try:
        from pyaux.runlib import init_logging
        init_logging(level=logging.DEBUG, app_name='dlsstaffimport')
    except Exception:
        logging.basicConfig(level=logging.DEBUG)

    worker = StaffImporter()
    func = getattr(worker, meth)
    LOG.info("%r.%s()", worker, meth)
    try:
        return func()
    except Exception:
        ei = sys.exc_info()
        maybe_postmortem(ei)
        raise


if __name__ == '__main__':
    main()

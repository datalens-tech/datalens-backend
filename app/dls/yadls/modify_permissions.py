# coding: utf8
"""
...
"""


from __future__ import annotations

from .exceptions import NotFound
from .utils import first_or_default, make_uuid, map_permissions, flatten_permissions, groupby
# from .node_classes import SubjectGrantWrap


class ModifyPermissionsManager:
    """
    A logic / utility class that implements the permissions modification.
    """

    class NotAllowed(Exception):
        """ ... """

    class NotConsistent(Exception):
        """ ... """

    NotFound = NotFound

    default_action = 'edit_permissions'
    diff_keys = ('added', 'removed', 'modified')

    def __init__(
            self, node, requester, diff, perms_orig, editable,
            perm_kind_sizes, perm_kind_titles,
            clear_all=False, default_comment=None, action=None,
            id_to_subject=None, name_to_subject=None,
            context=None, extras_for_log=None, extras_for_new_grants=None,
    ):
        self.node = node
        self.requester = requester
        self.diff = diff
        self.perms_orig = perms_orig
        self.editable = editable
        self.context = context or {}
        self.extras_for_log = extras_for_log or {}
        # Simlar to the 'description' handling,
        # primarily used for 'initial on-create':
        self.extras_for_new_grants = extras_for_new_grants or {}

        self.perm_kind_sizes = perm_kind_sizes
        self.perm_kind_titles = perm_kind_titles

        self.clear_all = clear_all
        self.default_comment = default_comment
        self.action = action or self.default_action
        self.id_to_subject = dict(id_to_subject or {})
        self.name_to_subject = name_to_subject or {}

        self.to_delete = []
        self.to_upsert = {}  # guid -> data
        self.to_logs = []
        self.perms_current = map_permissions(self.perms_orig, lambda item, **kwargs: item)

    def handle(self):
        self.postvalidate_diff(diff=self.diff)
        self.handle_removed()
        # Order is uncertain: there might be a use-case for 'move away and add another in its place' or something.
        self.handle_added()
        self.handle_modified()
        # TODO: check that acl_adm is nonempty (if it was nonempty before).

    @classmethod
    def postvalidate_diff(cls, diff):
        assert isinstance(diff, dict), "diff should be a dict"
        assert not set(diff) - set(cls.diff_keys), "diff should have no extra keys"
        grants_grouped = groupby(
            (
                # key
                (
                    # implicit: node
                    ('subject', item.name),
                    ('perm_kind', perm_kind),
                ),
                # value piece
                item,
            )
            for diff_key, perms in diff.items()
            for perm_kind, items in perms.items()
            for item in items)
        grants_duplicated = {
            key: val for key, val in grants_grouped.items()
            if len(val) > 1}
        if grants_duplicated:
            raise cls.NotConsistent(dict(
                message="Duplicate subjects in the diff data",
                subjects=list(dict(key)['subject'] for key in grants_duplicated),
            ))

    @staticmethod
    def is_subject_match(subject, other_subject):
        # if subject.id and other_subject.id: return subject.id == other_subject.id
        return subject.name == other_subject.name

    @classmethod
    def find_subject_matches(cls, subject, subject_list):
        return (
            other_subject for other_subject in subject_list
            if cls.is_subject_match(subject, other_subject))

    @classmethod
    def find_subject_match_simple(cls, subject, subject_list, **kwargs):
        return first_or_default(
            cls.find_subject_matches(subject, subject_list, **kwargs))  # type: ignore  # TODO: fix

    @classmethod
    def find_subject_match(cls, subject, subject_list, **kwargs):
        """
        `find_subject_matches` but expecting to find at most one.
        """
        results = cls.find_subject_matches(  # type: ignore  # TODO: fix
            subject=subject, subject_list=subject_list, **kwargs)
        results = iter(results)
        try:
            result = next(results)
        except StopIteration:
            return None
        try:
            extraneous_result = next(results)
        except StopIteration:
            pass
        else:
            raise Exception(
                "Multiple matching subjects were found while expecting one",
                (result, extraneous_result))
        return result

    def make_new_grant(self, description, extras=None, **kwargs):
        meta = kwargs.get('meta') or {}
        if extras is None:
            extras = dict(self.extras_for_new_grants)
        result = dict(
            guid=make_uuid(),
            node_config_id=self.node.node_config_id,
            meta=dict(
                meta,
                requester=self.requester.name,
                description=description,
                extras=extras,
            ))
        if self.editable:
            self.activate_new_grant(result, inplace=True)
        else:  # elif not self.editable:
            result.update(active=False, state='pending')
        result.update(kwargs)
        return result

    def _copy_grant_data(self, grant_data):
        grant_data = grant_data.copy()
        grant_data['meta'] = grant_data['meta'].copy()
        return grant_data

    def activate_new_grant(self, new_grant, set_approver=True, inplace=False):
        if not inplace:
            new_grant = self._copy_grant_data(new_grant)
        new_grant['active'] = True
        new_grant['state'] = 'active'
        if set_approver:
            new_grant['meta'].update(approver=self.requester.name)
        return new_grant

    def deactivate_grant_data(self, grant_data, set_remover=True, inplace=False):
        if not inplace:
            grant_data = self._copy_grant_data(grant_data)
        grant_data['active'] = False
        grant_data['state'] = 'deleted'
        if set_remover:
            grant_data['meta'].update(remover=self.requester.name)
        return grant_data

    def delete_grant(self, grant, comment=None, add_log=True, situation='etcetera', **kwargs):
        grant_data = grant.as_grant_data_()
        if grant_data['state'] == 'deleted':
            return dict(grant_data=grant_data, already_deactivated=True)
        self.deactivate_grant_data(grant_data, inplace=True)
        if comment is not None:
            grant_data['meta'].update(remover_comment=comment)
        if add_log:
            self.add_log(
                grant_data, existing_grant=grant,
                situation=situation, comment=comment,
                **kwargs)
        self.add_to_upsert(grant_data)
        return dict(grant_data=grant_data)

    @classmethod
    def normalize_grant_data(cls, grant):
        if not grant:
            return None
        if hasattr(grant, 'as_grant_data_'):
            grant = grant.as_grant_data_(subject=grant)
        return grant

    def grant_log_data(self, grant):
        grant = self.normalize_grant_data(grant)
        if not grant:
            return None
        result = dict(
            subject_id=grant['subject_id'],
            perm_kind=grant['perm_kind'],
            active=grant['active'],
            state=grant['state'],
            description=grant['meta'].get('description'),
            # # Not particularly relevant to each grant history item (should be
            # # obvious from other data):
            # requester=grant['meta'].get('requester'),
            # approver=grant['meta'].get('approver'),
        )
        if self.id_to_subject is not None:
            subject = self.id_to_subject.get(grant['subject_id'])
            if subject is not None:
                result['subject_name'] = subject.name
        return result

    def add_log(
            self, grant, comment, existing_grant, subject=None,
            grant_data_override=None, extras=None, **kwargs):
        """
        ...

        Note: pass `existing_grant=None` explicitly if there really was no grant before.
        """
        # TODO: ensure this catch is unnecessary.
        if subject is not None and subject.id and subject.id not in self.id_to_subject:
            self.id_to_subject[subject.id] = subject

        if comment is None:
            comment = self.default_comment

        grant = self.normalize_grant_data(grant)

        grant_data = self.grant_log_data(grant)
        if grant_data_override:
            grant_data.update(grant_data_override)

        grant_data_prev = self.grant_log_data(existing_grant)
        if grant_data_prev and grant_data:
            # Only save the modified values in the `prev`.
            grant_data_prev = {
                key: val for key, val in grant_data_prev.items()
                if grant_data.get(key) != val}

        result = dict(
            kind='grant_modify',
            sublocator='',
            grant_guid=grant['guid'],
            request_user_id=self.requester.id,
            node_identifier=self.node.identifier,
            meta={
                **dict(
                    context=self.context,
                    action=self.action,
                    request_user_name=self.requester.name,
                    grant_data=grant_data,
                    grant_data_prev=grant_data_prev,
                    comment=comment,
                    extras={
                        **(self.extras_for_log or {}),
                        **(extras or {}),
                    },
                ),
                **kwargs,
            },
        )
        self.to_logs.append(result)
        return result

    def _handle_common(self, data, perms_current=None, with_existing_around=False):
        if perms_current is None:
            perms_current = self.perms_current

        for perm_kind, diff_subjects in data.items():

            # Matches by (node, perm_kind)
            perms_current_here = perms_current.get(perm_kind, [])

            # Matches by node, differs by perm_kind
            if with_existing_around:
                perms_current_around = perms_current.copy()
                perms_current_around.pop(perm_kind, None)
                perms_current_around = flatten_permissions(perms_current_around)

            for diff_subject in diff_subjects:
                # Existing grant that matches by the (node, perm_kind, subject) which is unique (database-enforced).
                # (node, perm_kind) -> (node, perm_kind, subject)
                existing_grant = self.find_subject_match(
                    subject=diff_subject,
                    subject_list=perms_current_here,
                )

                item = dict(
                    perm_kind=perm_kind,
                    diff_subject=diff_subject,
                    existing_grant=existing_grant,
                )

                # Existing grants that match by (node, subject) and differ by perm_kind.
                if with_existing_around:
                    item.update(
                        existing_grants_around=list(self.find_subject_matches(
                            subject=diff_subject, subject_list=perms_current_around)),
                        perms_current=flatten_permissions(perms_current),
                    )

                yield item

    def handle_added(self, added=None):
        if added is None:
            added = self.diff.get('added') or {}

        for item in self._handle_common(added, with_existing_around=True):
            self.handle_added_one(**item)

    def make_dedup_info(self, pre_perm_kind, req_perm_kind, res_perm_kind):
        return dict(
            extras=dict(
                dedup=dict(
                    pre=pre_perm_kind,
                    req=req_perm_kind,
                    res=res_perm_kind),
            ),
        )

    def largest_grant(self, grants):
        if not grants:
            return None
        return max(grants, key=lambda grant: self.perm_kind_sizes[grant.perm_kind])

    def deduplicate_grants(self, perm_kind, existing_grant, existing_grants_around):
        result = dict(skip_new=False)
        existing_grants_around = list(existing_grants_around)
        if not existing_grants_around:
            # Nothing here to touch.
            return result

        perm_kind_sizes = self.perm_kind_sizes
        new_size = perm_kind_sizes[perm_kind]

        larger_active = [
            grant
            for grant in existing_grants_around
            if grant.grant_active and perm_kind_sizes[grant.perm_kind] > new_size]

        if larger_active:
            # Do not add the requested grant (regardless of `editable`).
            result.update(skip_new=True)
            # And write about it.
            grant = self.largest_grant(larger_active)
            log_info = self.make_dedup_info(
                pre_perm_kind=grant.perm_kind,
                req_perm_kind=perm_kind,
                res_perm_kind=grant.perm_kind)
            # NOTE: adding over an existing grant, but specifying the requested perm_kind. Hax-tuning, alas.
            self.add_log(
                # Not passing the existing grant since it does not really change.
                grant, existing_grant=None,
                situation='some_request_ignored_because_of_existing_larger',
                grant_data_override=dict(perm_kind=perm_kind),
                comment=None,
                **log_info)
        elif self.editable:  # and not larger_active:
            smaller = [
                grant
                for grant in existing_grants_around
                if perm_kind_sizes[grant.perm_kind] < new_size]
            for grant in smaller:
                log_info = self.make_dedup_info(
                    pre_perm_kind=grant.perm_kind,
                    req_perm_kind=perm_kind,
                    res_perm_kind=perm_kind)
                self.delete_grant(
                    grant,
                    situation='grant_override_by_larger',
                    comment=None,
                    **log_info)
        else:  # elif not self.editable and not larger_active:
            # Nothing is going to get deleted for a new *pending* request.
            # But maybe need to ignore the request.
            pass

        return result

    def handle_added_one(
            self, perm_kind, diff_subject, existing_grant, existing_grants_around,
            **kwargs):
        comment = diff_subject.info_['comment']

        # if (existing_grant_ext is not None and existing_grant is None and
        #         (existing_grant_ext.grant_active or existing_grant_ext.grant_state == 'pending')):
        #     raise self.NotConsistent(dict(
        #         message="A grant for the same subject already exists.",
        #         requested=dict(
        #             perm_kind=perm_kind
        #         ),
        #         existing=dict(
        #             perm_kind=existing_grant_ext.perm_kind,
        #         ),
        #     ))

        # editable => state is going to get modified => deduplicate first.
        dedup_res = self.deduplicate_grants(
            perm_kind=perm_kind,
            existing_grant=existing_grant,
            existing_grants_around=existing_grants_around)

        if dedup_res['skip_new']:
            return

        if existing_grant is not None:

            if existing_grant.grant_active:
                # if self.editable:
                self.add_log(
                    existing_grant, existing_grant=existing_grant,
                    comment=comment, situation='noop__grant_active_rerequest')
                return  # skip the whole diff_subject

            if existing_grant.grant_state == 'pending':
                if not self.editable:
                    # # append to the comment, change nothing else:
                    # new_grant = existing_grant.as_grant_data_()
                    # new_grant['meta']['comment'] = '{} | {}'.format(
                    #     new_grant['meta']['comment'],
                    #     diff_subject.info_['comment'])
                    # self.add_to_upsert(new_grant)
                    self.add_log(
                        existing_grant, existing_grant=existing_grant,
                        comment=comment, situation='noop__grant_pending_rerequest')
                    return

                # else:  # if editable:
                new_grant = existing_grant.as_grant_data_()
                # Kept: requester, description
                self.activate_new_grant(new_grant, inplace=True)
                self.add_log(
                    new_grant, existing_grant=existing_grant,
                    situation='grant_pending_approve', comment=comment)
                self.add_to_upsert(new_grant)
                return

            # else:  # 'former access', return it (to pending or active).
            situation = 'grant_return'
            new_grant = self.make_new_grant(
                guid=existing_grant.grant_guid,
                description=comment,  # new description
                extras={},  # Clear it.
                subject_id=diff_subject.id,
                perm_kind=perm_kind)

        # elif existing_grants_around None:  # and existing_grant is None:
        #     situation = 'grant_return_modified'
        #     existing_grant = existing_grant_ext
        #     new_grant = self.make_new_grant(
        #         guid=existing_grant.grant_guid,
        #         description=comment,  # new description
        #         subject_id=diff_subject.id,
        #         perm_kind=perm_kind)
        else:  # elif existing_grant is None:
            situation = 'grant_new_add'
            new_grant = self.make_new_grant(
                # Applied inside: `self.editable`.
                description=comment,
                subject_id=diff_subject.id,
                perm_kind=perm_kind)

        self.add_log(
            new_grant, existing_grant=existing_grant,
            subject=diff_subject,
            comment=comment, situation=situation)
        self.add_to_upsert(new_grant)

    def handle_removed(self, removed=None):
        if removed is not None:
            pass
        elif self.clear_all:
            removed = self.perms_current
        else:
            removed = self.diff.get('removed') or {}

        for item in self._handle_common(removed):
            self.handle_removed_one(**item)

    def handle_removed_one(
            self, perm_kind, diff_subject, existing_grant,
            situation='grant_existing_remove',
            **kwargs):
        """
        ...

        NOTE: also used from `handle_modified_one`.
        """
        if existing_grant is None:
            return  # Nothing to remove.

        assert perm_kind

        comment = diff_subject.info_.get('comment')
        extras = {}  # type: ignore  # TODO: fix
        if self.clear_all:
            extras.update(replace_all=True)

        if not self.editable:
            # Requesting to remove an access.
            # Allowed for the requester and for the target.
            if self.requester.name == existing_grant.grant_meta.get('requester'):
                situation = 'grant_existing_remove_by_requester'
            elif self.requester.name == existing_grant.name:
                situation = 'grant_existing_remove_by_subject'
            else:
                raise self.NotAllowed(dict(
                    message='Not allowed to remove grants here',
                    # grant_data=diff_subject.info_['original_spec'],
                    existing_grant=bool(existing_grant),
                ))
                # continue

        self.delete_grant(existing_grant, comment=comment, situation=situation, extras=extras)

    def handle_modified(self, modified=None):
        if modified is None:
            modified = self.diff.get('modified') or {}
        for item in self._handle_common(modified, with_existing_around=True):
            self.handle_modified_one(**item)

    def handle_modified_one(
            self, perm_kind, diff_subject, existing_grant, perms_current,
            **kwargs):

        assert perm_kind

        grant_key = dict(subject=diff_subject.name, perm_kind=perm_kind)
        if existing_grant is None:
            raise self.NotFound('A grant specified in `modify` was not found: {}'.format(grant_key))

        # NOTE: assuming that the permissions themselves are public to all
        # authenticated users.
        situation = 'grant_existing_modify'

        # TODO?: allow modifications of the pending grants by the requester.
        # (only okay as long as confirmation does not happen by the grant id)
        if not self.editable:
            raise self.NotAllowed(dict(
                message='Not allowed to modify grants here',
            ))

        comment = diff_subject.info_['comment']
        new_data = diff_subject.info_['new']
        new_grant = existing_grant.as_grant_data_()

        # ### First, handle the location changes ###

        new_subject_name = new_data.get('subject')
        if new_subject_name:
            new_subject = self.name_to_subject.get(new_subject_name)
            if not new_subject:
                raise self.NotFound(
                    'A specified subject was not found: {!r}'.format(
                        new_subject_name))
            new_grant['subject_id'] = new_subject.id

        new_perm_kind = new_data.get('grantType')
        if new_perm_kind:
            new_grant['perm_kind'] = new_perm_kind

        # Check whether the resulting target already exists / existed:
        grants_at_target = [
            grant
            for grant in perms_current
            if new_grant['subject_id'] == grant.subject.id and
            new_grant['perm_kind'] == grant.perm_kind and
            new_grant['id'] != grant.grant_id]

        if grants_at_target:

            # Cannot modify to place a grant over an existing active one.
            if any(grant.grant_active for grant in grants_at_target):
                raise self.NotConsistent(dict(
                    message="A grant for the specified subject and grantType already exists.",
                    requested=dict(
                        perm_kind=new_perm_kind,
                        subject=new_subject_name)))

            # else: A non-active grant in `grants_at_target`

            # Simulate the 'remove the previous add the new'

            assert len(grants_at_target) == 1, "should be ascertained by the database constraints"
            grant_at_target = grants_at_target[0]

            # Remove the specified grant
            self.delete_grant(existing_grant, add_log=False)

            # Set up the modification as reactivation:
            # existing_grant = grant_at_target_perm_kind
            new_grant = grant_at_target.as_grant_data_()
            self.activate_new_grant(new_grant, inplace=True)
            new_grant['meta'].update(requester=self.requester.name)
            situation = 'modify_replace_reactivate'

        # ### Handle the property changes ###

        for key in ('description',):
            new_value = new_data.get(key)
            if new_value is not None:
                new_grant['meta'][key] = new_value

        self.add_log(
            new_grant, existing_grant=existing_grant,
            comment=comment, situation=situation)
        self.add_to_upsert(new_grant)

    def add_to_upsert(self, grant_data, update_existing=True):

        grant_guid = grant_data['guid']

        if update_existing:

            def mapper(item, **kwargs):
                if item.grant_guid == grant_guid:
                    # Replace by guid
                    # return SubjectGrantWrap.from_grant_data_(grant_data)
                    return item.__class__.from_grant_data_(grant_data)
                return item

            self.perms_current = map_permissions(self.perms_current, mapper)

        self.to_upsert[grant_guid] = grant_data

        return grant_data

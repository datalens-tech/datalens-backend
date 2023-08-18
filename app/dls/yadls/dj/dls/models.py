"""
Django models description.

WARNING: debug/maintenance only. The main models code is in `yadls.db` module.
"""

from __future__ import annotations

from django.db import models
import django.contrib.postgres.fields as pg

__all__ = (
    'Node',
    'NodeConfig',
    'Subject',
    'group_members_m2m',
    'subject_memberships_m2m',
    'Grant',
    'Log',
    'Data',
    'PeriodicTask',
    'SSWord',
    'ss_word_subjects_m2m',
)


def _get_attr(obj, path):
    if isinstance(path, str):
        path = path.split('.')
    result = obj
    for key in path:
        result = getattr(result, key)
    return result


class Base(models.Model):
    class Meta:
        # app_label = 'dls'
        abstract = True

    _repr_fields: tuple[str, ...] = ('pk',)

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join(
                '{}={!r}'.format(key, _get_attr(self, key))
                for key in self._repr_fields))


# def _pk_col(**kwargs):
#     return models.AutoField(primary_key=True)


def _timestamp_col(precision=6, timezone=True, **kwargs):
    assert timezone
    # TODO: precision
    return models.DateTimeField(**kwargs)


def _ctime_col(**kwargs):
    return _timestamp_col(auto_now_add=True, db_index=True, **kwargs)


def _mtime_col(**kwargs):
    return _timestamp_col(auto_now=True, db_index=True, **kwargs)


def _meta_col(**kwargs):
    return pg.JSONField(default=dict, **kwargs)


def _realm_col(**kwargs):
    return models.TextField(default='', null=False, db_index=True, **kwargs)


class Node(Base):

    class Meta:
        db_table = 'dls_nodes'

    _repr_fields = ('id', 'identifier', 'scope', 'realm')

    # id = _pk_col()
    identifier = models.TextField(db_index=True, null=False, unique=True)
    scope = models.TextField(db_index=True, null=False)
    meta = _meta_col()
    ctime = _ctime_col()
    realm = _realm_col()


class CommonBase(Base):
    class Meta:
        abstract = True

    # id = _pk_col()
    meta = _meta_col()
    ctime = _ctime_col()
    mtime = _mtime_col()
    realm = _realm_col()


class NodeConfig(CommonBase):

    class Meta:
        db_table = 'dls_node_config'

    _repr_fields = ('id', 'node_identifier', 'scope', 'realm')

    node_identifier = models.TextField(db_index=True, null=False, unique=True)
    scope = models.TextField(db_index=True, null=False, default='')

    # node_id = ...
    node = models.OneToOneField(
        'Node',
        on_delete=models.CASCADE,
        related_name='node_config',
        null=True,
    )


class Subject(CommonBase):

    class Meta:
        db_table = 'dls_subject'

    _repr_fields = ('id', 'kind', 'name', 'realm')

    # XXX: subject_kind enum
    kind = models.TextField(db_index=True, null=False)

    name = models.TextField(db_index=True, null=False, unique=True)

    @property
    def title(self):
        login = self.meta.get('_login')
        title = self.meta.get('title')
        return '{} ({} {})'.format(self.name, login or '-', title or '-')

    # node_condig_id = ...
    node_config = models.OneToOneField(
        'NodeConfig',
        on_delete=models.CASCADE,
        related_name='subject',
    )

    active = models.BooleanField(default=True, db_index=True, null=False)
    source = models.TextField(db_index=True, null=False, default='unknown')

    search_weight = models.IntegerField(db_index=True, null=False, default=0)

    members = models.ManyToManyField(
        'Subject',
        through='group_members_m2m',
        through_fields=('group', 'member'),
        related_name='memberships_direct',
    )
    memberships = models.ManyToManyField(
        'Subject',
        through='subject_memberships_m2m',
        through_fields=('subject', 'group'),
        related_name='members_recursive',
    )


class group_members_m2m(Base):

    class Meta:
        db_table = 'dls_group_members_m2m'
        unique_together = ('group', 'member')

    _repr_fields = ('group_id', 'member_id', 'source', 'realm')
    # _repr_fields = ('group', 'member', 'source', 'realm')

    # group_id = '...'
    group = models.ForeignKey(
        'Subject',
        on_delete=models.CASCADE,
        related_name='_members_m2m+',
    )

    # member_id = '...'
    member = models.ForeignKey(
        'Subject',
        on_delete=models.CASCADE,
        related_name='_memberships_direct_m2m+',
    )

    source = models.TextField(db_index=True, null=False, default='unknown')
    meta = _meta_col()
    ctime = _ctime_col()
    mtime = _mtime_col()
    realm = _realm_col()


class subject_memberships_m2m(Base):

    class Meta:
        db_table = 'dls_subject_memberships_m2m'
        unique_together = ('subject', 'group')

    _repr_fields = ('subject_id', 'group_id', 'realm')
    # _repr_fields = ('subject', 'group', 'realm')

    # subject_id = ...
    subject = models.ForeignKey(
        'Subject',
        on_delete=models.CASCADE,
        related_name='_memberships_m2m+',
    )
    # group_id = ...
    group = models.ForeignKey(
        'Subject',
        on_delete=models.CASCADE,
        related_name='_members_recursive_m2m+',
    )
    realm = _realm_col()


class Grant(CommonBase):

    class Meta:
        db_table = 'dls_grant'
        # # Removing:
        # unique_together = ('node_config', 'subject')

    # _repr_fields = ('guid', 'state', 'perm_kind', 'node_config_id', 'subject_id', 'realm')
    _repr_fields = ('guid', 'state', 'perm_kind', 'node_config_id', 'node_config.node_identifier', 'subject_id', 'subject.title', 'realm')

    guid = models.UUIDField(unique=True, db_index=True)

    perm_kind = models.TextField(null=False, db_index=True)

    # node_config_id = ...
    node_config = models.ForeignKey(
        'NodeConfig',
        on_delete=models.CASCADE,
        related_name='permissions_subjects',
    )
    # subject_id = ...
    subject = models.ForeignKey(
        'Subject',
        on_delete=models.CASCADE,
        related_name='node_permissions',
    )

    active = models.BooleanField(db_index=True, null=False)
    # default | requested | ...
    state = models.TextField(default='default', db_index=True, null=False)


class Log(CommonBase):

    class Meta:
        db_table = 'dls_log'

    _repr_fields = (
        'ctime', 'kind', 'sublocator', 'grant_id',
        # 'request_user_id',
        'request_user.title',
        # 'node_identifier',
    )

    kind = models.TextField(default='etc', db_index=True, null=False)

    sublocator = models.TextField(default='', db_index=True, null=False)

    # grant_guid = models.UUIDField(
    #     db_index=True, null=True)
    grant = models.ForeignKey(
        'Grant',
        on_delete=models.CASCADE,
        related_name='logs',
        db_column='grant_guid',
        to_field='guid',
        null=True,
    )

    # request_user_id = ...
    request_user = models.ForeignKey(
        'Subject',
        on_delete=models.CASCADE,
        related_name='logs_as_request_user',
        null=True,
    )

    # Convenience, should be a copy of the `sublocator` data.
    # node_identifier = models.TextField(
    #     db_index=True, null=True)
    # node = models.ForeignKey(
    #     'Node',
    #     on_delete=models.CASCADE,
    #     related_name='logs',
    #     db_column='node_identifier',
    #     to_field='identifier',
    #     null=True,
    # )
    node_config = models.ForeignKey(
        'NodeConfig',
        on_delete=models.CASCADE,
        related_name='logs',
        db_column='node_identifier',
        to_field='node_identifier',
        null=True,
    )


class Data(CommonBase):

    class Meta:
        db_table = 'dls_data'

    # id = ...
    key = models.TextField(db_index=True, null=False, unique=True)
    data = models.BinaryField(null=False)
    # meta = ...


class PeriodicTask(Base):

    class Meta:
        db_table = 'dls_periodic_task'

    name = models.TextField(primary_key=True)

    # settings
    frequency = models.FloatField()
    lock_expire = models.FloatField()
    lock_renew = models.FloatField()

    # state
    lock = models.TextField(null=True)
    last_start_ts = _timestamp_col(null=True)
    last_success_ts = _timestamp_col(null=True)
    last_failure_ts = _timestamp_col(null=True)
    last_ping_ts = _timestamp_col(null=True)
    last_ping_meta = _meta_col()

    meta = _meta_col()
    realm = _realm_col()


class ss_word_subjects_m2m(Base):

    class Meta:
        db_table = 'dls_ss_word_subjects'
        unique_together = ('ssword', 'subject')

    # ssword_id = ...
    ssword = models.ForeignKey(
        'SSWord',
        on_delete=models.CASCADE,
        related_name='subjects+',
    )
    # subject_id = ...
    subject = models.ForeignKey(
        'Subject',
        on_delete=models.CASCADE,
        related_name='sswords+',
    )

    realm = _realm_col()


class SSWord(Base):

    class Meta:
        db_table = 'dls_ss_word'

    # id = _pk_col()
    word = models.TextField(db_index=True, null=False, unique=True)

    subjects = models.ManyToManyField(
        'Subject',
        through='ss_word_subjects_m2m',
        # through_fields=('ssword', 'subject'),
        related_name='sswords',
    )

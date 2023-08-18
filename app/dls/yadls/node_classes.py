""" ... """

from __future__ import annotations

from bi_utils.utils import DotDict

from .utils import map_permissions, flatten_permissions


class WrapCommon:
    # NOTE: `_attr` is 'private', `attr_` is to avoid namespace collision.
    _key_attrs = ('_wrapped',)
    _single_key_straight_repr = True
    info_ = None

    @property
    def _repr_keys(self):
        return self._key_attrs

    @property
    def _data(self):
        return dict(self._wrapped, meta=dict(self._wrapped.meta))  # type: ignore  # TODO: fix

    def __init__(self, wrapped, info=None):
        info = dict(info or {})  # always copy

        # Auto-unroll:
        if isinstance(wrapped, self.__class__):
            orig_wrapped = wrapped
            wrapped = wrapped._wrapped  # type: ignore  # TODO: fix
            info = {
                **orig_wrapped.info_,  # type: ignore  # TODO: fix
                **{"_orig_wrapped": orig_wrapped},
                **info}

        self._wrapped = wrapped
        self.info_ = info

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def __getitem__(self, name):
        return getattr(self._wrapped, name)

    def __repr__(self):
        repr_keys = self._repr_keys
        if self._single_key_straight_repr and len(repr_keys) == 1:
            return str(getattr(self, repr_keys[0]))
        return "<{}({})>".format(
            self.__class__.__name__,
            ", ".join(
                "{}={!r}".format(key, getattr(self, key))
                for key in repr_keys))

    def _key(self):
        return tuple(getattr(self, key) for key in self._key_attrs)

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other):
        return (
            # Inter-type comparison with arbitrary keys is dangerous.
            type(other) is type(self) and
            self._key() == other._key())

    def __lt__(self, other):
        if type(other) is type(self):
            return self._key() < other._key()
        return NotImplemented

    def __le__(self, other):
        if type(other) is type(self):
            return self._key() <= other._key()
        return NotImplemented


class NodeWrap(WrapCommon):
    _key_attrs = ('identifier',)
    _permissions = None

    def get_permissions(self, **kwargs):
        """ Get the permissions with `SubjectWrap` instances """
        result = self._permissions
        return map_permissions(
            result,
            lambda val, **kwargs: (SubjectWrap(val) if val.grant_active else None))

    def get_permissions_ext(self, **kwargs):
        """ Get the permissions with `SubjectGrantWrap` instances """
        return self._permissions

    def set_permissions(self, value, _check=True, **kwargs):
        if _check:
            assert all(
                isinstance(subject, SubjectGrantWrap)
                for subject in flatten_permissions(value))
        self._permissions = value


class SubjectWrap(WrapCommon):
    _key_attrs = ('name',)
    _effective_groups = None

    def get_effective_groups(self, **kwargs):
        return self._effective_groups

    def set_effective_groups(self, value, **kwargs):
        assert isinstance(value, (list, tuple, set))
        assert all(isinstance(item, SubjectWrap) for item in value)
        self._effective_groups = set(value)


class SubjectGrantWrap(SubjectWrap):

    _key_attrs = ('name', 'perm_kind')  # type: ignore  # TODO: fix

    @property
    def subject(self):
        return SubjectWrap(self)

    def as_grant_data_(self, **kwargs):
        return dict(
            kwargs,

            # Locator:
            node_config_id=self.node_config_id,
            perm_kind=self.perm_kind,
            subject_id=self.id,
            # ...
            guid=self.grant_guid,
            active=self.grant_active,
            state=self.grant_state,

            # Optional-ish:
            id=self.grant_id,
            meta=dict(self.grant_meta),
            ctime=self.grant_ctime,
            mtime=self.grant_mtime,

            subject=dict(
                id=self.id,
                kind=self.kind,
                name=self.name,

                # Optional-ish:
                source=getattr(self, 'source', None),
                meta=getattr(self, 'meta', None),
                ctime=getattr(self, 'ctime', None),
                mtime=getattr(self, 'mtime', None),
            ),
        )

    @classmethod
    def from_grant_data_(cls, data):
        subject = data.get('subject') or {}
        meta = data.get('meta')
        self_data = dict(
            node_config_id=data['node_config_id'],
            perm_kind=data['perm_kind'],
            id=data['subject_id'],

            grant_guid=data['guid'],
            grant_active=data['active'],
            grant_state=data['state'],

            grant_id=data.get('id'),
            grant_meta=dict(meta) if meta is not None else None,
            grant_ctime=data.get('ctime'),
            grant_mtime=data.get('mtime'),

            kind=subject.get('kind'),
            name=subject.get('name'),
            source=subject.get('source'),
            meta=subject.get('meta'),
            ctime=subject.get('ctime'),
            mtime=subject.get('mtime'),
        )
        return cls(DotDict(self_data))


# class GrantWrap(WrapCommon):
#     _key_attrs = ('id',)

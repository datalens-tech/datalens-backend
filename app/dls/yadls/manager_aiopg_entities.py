from __future__ import annotations

from typing import Optional, Sequence

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg

from bi_utils.aio import alist
from bi_utils.utils import DotDict

from . import db as db_base, db_optimizations, db_walkers
from .db_utils import db_get_one, db_insert_one, get_or_create_aio
from .exceptions import NotFound
from .manager_aiopg_base import DLSPGDB, ensure_db_reading, ensure_db_writing
from .node_classes import SubjectWrap
from .utils import groupby, make_uuid


class DLSPGEntities(DLSPGDB):
    """ Entity interaction support functions """

    @ensure_db_reading
    async def _get_node_async(
            self, node, fetch_permissions=True, for_update=False,
            assume_entry=False, assume_subject=False):
        """

        :param fetch_permissions: ensure an attribute required for synchronous
        `self.get_node_permissions(node)` is populated.
        """
        Node = self.Node
        NodeConfig = self.NodeConfig
        node_spec = node
        select = None

        base = NodeConfig.__table__
        entry = False
        subject = False
        if for_update:
            pass
        elif assume_entry:
            base = base.join(Node)
            entry = True
        elif assume_subject:
            base = base.join(self.Subject)
            subject = True
        else:
            base = base.outerjoin(Node)
            base = base.outerjoin(self.Subject)
            entry = True
            subject = True

        if isinstance(node, str):
            select = base.select(NodeConfig.node_identifier == node)

        if select is not None:
            columns = self._node_columns(entry=entry, subject=subject)
            select = select.with_only_columns(columns)
            if for_update:
                select = select.with_for_update()
            rows = await self.db_conn.execute(select.limit(2))
            node = await db_get_one(
                rows,
                not_found_msg="The specified node was not found: {!r}".format(node_spec))
            node = self.node_cls(node)  # type: ignore  # TODO: fix

        assert isinstance(node, self.node_cls)  # type: ignore  # TODO: fix

        if fetch_permissions:
            node = await self._prefetch_node_permissions_async(node)

        return node

    @ensure_db_reading
    async def _prefetch_node_permissions_async(self, node, active=True, perm_kinds=None, force=False, detailed=True):
        if node.get_permissions_ext() is not None and not force:
            return node

        # Node = self.Node
        NodeConfig = self.NodeConfig
        Grant = self.Grant

        base_table = self._subject_grant_from()
        columns = self._subject_grant_columns(detailed=detailed)

        node_identifier = node if isinstance(node, str) else node.identifier
        # base_table = base_table.join(Node)
        where = (NodeConfig.node_identifier == node_identifier)
        if active is not None:
            where = where & (Grant.active == active)
        if perm_kinds is not None:
            where = where & (Grant.perm_kind.in_(perm_kinds))
        select = base_table.select(where).with_only_columns(columns)
        rows = await self.db_conn.execute(select)
        rows = await alist(rows)
        perms = groupby(  # {perm_kind: [subject_obj, ...]}
            (row['perm_kind'], self.subject_grant_cls(row, info=dict(row=row))) for row in rows)
        node.set_permissions(perms)
        return node

    @ensure_db_reading
    async def _get_multi_nodes_async(self, nodes, fetch_permissions=True):
        """
        ...

        Reminder: might silently return less items than the len(nodes).
        """
        assert isinstance(nodes, (list, tuple))
        assert all(isinstance(node, str) for node in nodes), "should only work with nodeids here"
        Node = self.Node
        select = Node.select_(Node.identifier.in_(nodes))
        rows = await self.db_conn.execute(select.limit(db_base.BULK_ENTITY_LIMIT))
        rows = await alist(rows)
        nodes_objs = [self.node_cls(row) for row in rows]  # type: ignore  # TODO: fix
        if fetch_permissions:
            nodes_objs = await self._prefetch_multi_nodes_permissions_async(nodes_objs)
        return nodes_objs

    @ensure_db_reading
    async def _prefetch_multi_nodes_permissions_async(
            self, nodes, active=True, perm_kinds=None, force=False, detailed=True):
        NodeConfig = self.NodeConfig
        Grant = self.Grant
        base_table = self._subject_grant_from()
        columns = self._subject_grant_columns(detailed=detailed)
        columns += [NodeConfig.node_identifier]

        where = NodeConfig.node_identifier.in_(list(set(node.identifier for node in nodes)))
        if active is not None:
            where = where & (
                Grant.active == True  # noqa: E712  # pylint: disable=singleton-comparison
            )
        if perm_kinds is not None:
            where = where & (Grant.perm_kind.in_(perm_kinds))
        select = base_table.select(where).with_only_columns(columns)
        rows = await self.db_conn.execute(select)
        rows = await alist(rows)
        stuff = groupby((row['node_identifier'], row) for row in rows)
        stuff = {
            key: groupby(
                (row['perm_kind'], self.subject_grant_cls(row, info=dict(row=row)))
                for row in rows)
            for key, rows in stuff.items()}
        for node in nodes:
            node_perms = stuff.get(node.identifier, {})
            node.set_permissions(node_perms)
        return nodes

    @ensure_db_reading
    async def _get_subject_node_async(
            self, subject,
            expected_kind=None,
            autocreate=None,
            fetch_effective_groups=True,
            ensure_id=True,
            **kwargs):

        if autocreate is None:
            autocreate = self.autocreate_subjects

        if autocreate:
            assert expected_kind
            assert isinstance(subject, (str, self.subject_cls))  # type: ignore  # TODO: fix

        Subject = self.Subject
        subject_spec = subject
        select = None

        # Catch for `self.get_superuser_group` and `self.get_active_user_group`, basically.
        if ensure_id and isinstance(subject, self.subject_cls) and subject.id is None:  # type: ignore  # TODO: fix
            subject = subject.name
            subject_spec = subject

        if isinstance(subject, str):
            select = Subject.select_(
                (Subject.name == subject)
                # | (Subject.node_config.has(node_identifier=subject))
            )
        elif isinstance(subject, int):
            select = Subject.select_(Subject.id == subject)
        if select is not None:
            # if expected_kind is not None:
            #     select = select.where(Subject.kind == expected_kind)
            rows = await self.db_conn.execute(select.limit(2))
            try:
                subject = await db_get_one(
                    rows,
                    not_found_msg='The specified subject was not found: {!r}'.format(subject_spec))
            except NotFound:
                if not autocreate:
                    raise
                subject = await self._autocreate_subject(subject=subject_spec, kind=expected_kind)
            subject = self.subject_cls(subject)  # type: ignore  # TODO: fix
        if expected_kind is not None:
            # assert subject.kind == expected_kind
            if subject.kind != expected_kind:
                raise NotFound((
                    'The specified subject is of the wrong kind. Subject: {!r},'
                    ' kind: {!r}, expected kind: {!r}').format(subject_spec, subject.kind, expected_kind))
        if fetch_effective_groups:
            subject = await self._prefetch_subject_effective_groups_async(subject)
        return subject

    @ensure_db_reading
    async def _get_subjects_nodes_async(
            self, subjects: Sequence[str], expected_kind=None,
            autocreate=False, require=True,
            fetch_effective_groups=False,
            subject_objs: Optional[Sequence[db_base.Subject]] = None,
            **kwargs,
    ) -> list[db_base.Subject]:

        if not subjects:
            return []

        if autocreate:
            raise Exception("TODO")
        if fetch_effective_groups:
            raise Exception("TODO")

        assert all(isinstance(subject, str) for subject in subjects), "should only work with subject names here"

        Subject = self.Subject

        results: list[db_base.Subject] = []
        if subject_objs is not None:
            subj_map = {str(subj.name): subj for subj in subject_objs}
            ready_subjects = [name for name in subjects if name in subj_map]
            results.extend(subj_map[name] for name in ready_subjects)
            subjects = [name for name in subjects if name not in subj_map]

        if subjects:
            select = Subject.select_(Subject.name.in_(subjects))
            rows = await self.db_conn.execute(select)
            db_results = await alist(rows)
            results.extend(db_results)

        missing = None
        if require or autocreate:
            name_to_obj = {obj.name: obj for obj in results}
            missing = [subject for subject in subjects if subject not in name_to_obj]

        if autocreate and missing:
            raise Exception("TODO")

        elif require and missing:
            raise NotFound("Some of the subjects were not found: {}".format(missing))

        if expected_kind is not None:
            mismatch = list(subject.name for subject in results if subject.kind != expected_kind)
            if mismatch:
                raise NotFound((
                    'The specified subjects is of the wrong kind. Subjects: {!r},'
                    ' expected kind: {!r}').format(mismatch, expected_kind))

        if fetch_effective_groups:
            raise Exception("TODO")

        return results

    @ensure_db_writing
    async def _autocreate_subject(self, subject, kind, log_warn=True):
        assert isinstance(subject, (str, self.subject_cls))  # type: ignore  # TODO: fix
        assert kind
        assert isinstance(kind, str)

        NodeConfig = self.NodeConfig
        Subject = self.Subject

        # Point: have to make up a NodeConfig before writing the subject.
        async def on_before_create(values, **kwargs):
            node_identifier = None
            kind = None
            if not isinstance(subject, str):
                node_identifier = getattr(subject, 'node_identifier', None)
                kind = getattr(subject, 'kind', None)
            if node_identifier is None:
                node_identifier = str(make_uuid())
            if kind is None:
                kind = 'user'
            insert = NodeConfig.insert_().values(
                node_identifier=node_identifier,
                scope=kind,
            )
            # WARN: if another process creates the Subject at this time, the
            # NodeConfig will be left dangling.
            cfg_id = await db_insert_one(self.db_conn, insert)
            values['node_config_id'] = cfg_id

        if log_warn:
            self.logger.warning("Autocreating subject: %r, kind: %r", subject, kind)

        # If the current database is `local` (slave), ensure a master-host
        # connection and a transaction is used from this point.
        await self.db.ensure_db_writing()
        if isinstance(subject, str):
            data = dict(
                name=subject,
                kind=kind)
        else:
            _keys = ('kind', 'source', 'name')
            data = {
                key: getattr(subject, key, None)
                for key in _keys}
            data = {key: val for key, val in data.items() if val is not None}

        subject = await get_or_create_aio(
            conn=self.db_conn, table=Subject,
            key=['name'],
            values=data,
            on_before_create=on_before_create,
        )

        # If subject is a user subject, ensure it has the 'active' group.
        if kind == 'user':
            aug = self.get_active_user_group()
            if aug is not None:
                await self.add_group_subject_async(group=aug, subject=subject, autocreate_group=True)

        return subject

    @ensure_db_writing
    async def add_group_subject_async(self, group, subject, autocreate_group=False, meta=None):
        """

        Primarily used for `_autocreate_subject`.
        """
        group = await self._get_subject_node_async(
            group, expected_kind='group',
            autocreate=autocreate_group, fetch_effective_groups=False)
        subject = await self._get_subject_node_async(
            subject, autocreate=False, fetch_effective_groups=False)

        m2m = self.group_members_m2m
        if meta is None:
            meta = {}
        data = dict(group_id=group.id, member_id=subject.id, meta=meta)
        insert = sa_pg.insert(m2m).values(data).on_conflict_do_nothing()
        await self.db_conn.execute(insert)
        return data

    @ensure_db_reading
    async def _prefetch_subject_effective_groups_async(self, subject, force=False, optimized=True):
        if subject.get_effective_groups() is not None and not force:
            return subject

        Subject = self.Subject
        subject_id = subject if isinstance(subject, int) else subject.id

        if optimized and not isinstance(subject, int):
            rows = await self.db_conn.execute(
                db_optimizations.CHECK_PERMS_SQL_SUBJECT_GROUPS_WITH_CACHE_BY_ID,
                dict(subject=subject.name, subject_id=subject_id))
            rows = await alist(rows)
            rows = [row.data_ for row in rows if row.type_ == 'subject_group']
        else:
            gid_select = (
                sa.text(db_walkers.sql_effective_groups() % ':subject_id')
                .bindparams(subject_id=subject_id))
            select = Subject.select_().where(Subject.id.in_(gid_select))
            select = select.with_only_columns([Subject.name])

            rows = await self.db_conn.execute(select)
            rows = await alist(rows)

        groups = list(SubjectWrap(DotDict(row)) for row in rows)
        subject.set_effective_groups(groups)
        return subject

    def get_subject_effective_groups(self, subject, **kwargs):
        result = subject.get_effective_groups()
        if result is None:
            raise Exception("Subject effective groups should've been prefetched")
        return result

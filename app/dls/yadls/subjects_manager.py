""" ... """

from __future__ import annotations

import logging
import re
import time
from collections import defaultdict
from typing import Optional

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg

from bi_utils.utils import DotDict

from .utils import chunks, with_last, maybe_postmortem, uniq
from . import db as db_base
from .db_utils import iterate_by_pk, sa_all


LOG = logging.getLogger(__name__)


def _sanitize(value):
    if not value:
        return None
    value = str(value)
    # # should not be in a normal word, only in hax-prefixed words.
    # value = value.replace('_', ' ')
    value = value.strip()
    return value


class SubjectsManager:

    # db = db_base
    # models = db_base
    db_get_engine = staticmethod(db_base.get_engine)
    Subject = db_base.Subject
    SSWord = db_base.SSWord
    ss_word_subjects_m2m = db_base.ss_word_subjects_m2m

    db_engine: Optional[db_base.TDBEngine] = None
    languages = ('ru', 'en')
    insert_chunk_size = db_base.DEFAULT_INSERT_CHUNK_SIZE
    delete_chunk_size = db_base.DEFAULT_DELETE_CHUNK_SIZE

    def main(self) -> None:
        LOG.info("update_search_data()...")
        t1 = time.time()
        self.update_search_data()
        t2 = time.time()
        LOG.info("update_search_data() done in %.3fs.", t2 - t1)

    def update_search_data(self) -> None:
        db_engine = self.db_engine
        if db_engine is None:
            db_engine = self.db_get_engine()

        Word = self.SSWord
        m2m = self.ss_word_subjects_m2m
        m2m_pk = sa.sql.expression.tuple_(m2m.ssword_id, m2m.subject_id)

        words, words_subjects = self._prepare_search_words(db_conn=db_engine)
        LOG.debug("%r words, %r m2m", len(words), len(words_subjects))

        # TODO?: a temp-table binary-copy diff-update context-manager `(insert,
        # (insert, delete), delete)`
        stmt = Word.select_().with_only_columns([Word.word, Word.id])
        rows = db_engine.execute(stmt)
        word_to_id = dict(iter(rows))

        words_new = [word for word in words if word not in word_to_id]

        LOG.debug("%r words_current, %r words_new", len(word_to_id), len(words_new))

        stmt = (
            sa_pg.insert(Word)
            .on_conflict_do_nothing()
            .returning(Word.word, Word.id))
        for chunk in chunks(words_new, self.insert_chunk_size):
            rows = db_engine.execute(stmt.values([dict(word=word) for word in chunk]))
            word_to_id.update(iter(rows))

        m2m_data = set((word_to_id[word], sid) for word, sid in words_subjects)

        stmt = m2m.select_().with_only_columns([m2m.ssword_id, m2m.subject_id])
        rows = db_engine.execute(stmt)
        m2m_current = set((word_id, subject_id) for word_id, subject_id in rows)

        m2m_obsolete = m2m_current - m2m_data
        m2m_new = m2m_data - m2m_current
        LOG.debug('%r m2m_obsolete, %r m2m_new', len(m2m_obsolete), len(m2m_new))

        stmt = sa_pg.insert(m2m).on_conflict_do_nothing()
        for chunk in chunks(list(m2m_new), self.insert_chunk_size):
            db_engine.execute(stmt.values(
                [dict(ssword_id=ssword_id, subject_id=subject_id)
                 for ssword_id, subject_id in chunk]
            ))

        for chunk in chunks(list(m2m_obsolete), self.delete_chunk_size):
            stmt = m2m.delete_(m2m_pk.in_(chunk))
            db_engine.execute(stmt)

        # When obsolete m2m are cleared, can clear obsolete words.
        # This probably could've been a context manager or something.
        words_obsolete_set = set(word_to_id) - set(words)
        words_obsolete = [word_to_id[word] for word in words_obsolete_set]
        LOG.debug("%r words_obsolete", len(words_obsolete))

        for chunk in chunks(words_obsolete, self.delete_chunk_size):
            stmt = Word.delete_(Word.id.in_(chunk))
            db_engine.execute(stmt)

        LOG.debug("update_search_data: done")

    # No point in suggesting the superusers.
    _search_excluded_subject_names = set((
        'system_group:superuser',
        'system_user:root',
    ))

    def _prepare_search_words(self, db_conn):
        words: dict[str, int] = defaultdict(int)
        words_subjects = []

        tbl = self.Subject
        qs = tbl.select_(
            tbl.active == True  # noqa: E712  # pylint: disable=singleton-comparison
        )
        qs = iterate_by_pk(qs, db_conn=db_conn)
        for subject in qs:
            if subject.name in self._search_excluded_subject_names:
                continue
            subject_words_all = set()
            for language in self.languages:
                subject_info = self.subject_to_info(
                    subject, internal=True, language=language)
                subject_words = self.info_to_words(subject_info)
                subject_words = self.process_search_words(subject_words)
                assert isinstance(subject_words, set)
                subject_words_all.update(subject_words)

            for word in subject_words_all:
                words[word] += 1
                words_subjects.append((word, subject.id))

        return words, words_subjects

    staff_group_type_url_tpls = dict(
        department='https://staff.yandex-team.ru/departments/{}/',
        wiki='https://staff.yandex-team.ru/groups/{}/',
        # Special case.
        service='https://abc.yandex-team.ru/services/{}/',
        # Special special case.
        servicerole='https://abc.yandex-team.ru/services/{}/',
    )

    staff_user_link_tpl = 'https://staff.yandex-team.ru/{}'
    staff_user_icon_tpl = 'https://center.yandex-team.ru/api/v1/user/{}/photo/64/square.jpg'

    @classmethod
    def _staff_group_url(cls, subject):
        meta = subject.meta
        group_type = meta.get('type')

        url_tpl = cls.staff_group_type_url_tpls.get(group_type)
        if not url_tpl:
            return None

        # NOTE: expecting to find the original 'url' field under the 'name'
        if group_type == 'servicerole':
            url = (meta.get('parent') or {}).get('url_data')
        else:
            url = meta.get('url_data')

        if not url:
            return None

        return url_tpl.format(url)

    subject_type_mapping = {
        # (kind, source, type) -> value
        # (kind, source) -> value
        # (kind,) -> value
        ('group', 'staff', 'department'): 'group-staff-department',
        ('group', 'staff', 'wiki'): 'group-staff-wiki',
        ('group', 'staff', 'service'): 'group-staff-service',
        ('group', 'staff', 'servicerole'): 'group-staff-servicerole',
        ('group', 'staff'): 'group-staff-etc',
        ('user', 'staff'): 'user-staff',
        ('group', 'system'): 'group-system',
        ('user', 'system'): 'user-system',
        ('group',): 'group',
        ('user',): 'user',
        (): '???',
    }

    @classmethod
    def subject_to_info(cls, subject, internal=False, language='ru'):
        result = {}
        meta = subject.meta

        def _localize(value):
            if language is None or not isinstance(value, dict):
                return value
            return (
                value.get(language) or
                value.get('ru') or
                (list(value.values()) or [None])[0]
            )

        # Always return the identifier for unambiguity.
        result['name'] = subject.name

        # Internal use:
        result['__source'] = subject.source
        result['__rlsid'] = meta.get('__rlsid')

        # A 'full type' constant for the frontend.
        type_info = (subject.kind, subject.source, meta.get('type'))
        type_mapping = cls.subject_type_mapping
        subject_type_it = (
            type_mapping.get(type_info[:mlen])
            # from full to `()`
            for mlen in reversed(range(len(type_info) + 1)))
        subject_type_it = (item for item in subject_type_it if item is not None)
        subject_type = next(subject_type_it)

        result['type'] = subject_type

        url_data = meta.get('url_data') or subject.name.split(':', 1)[-1]
        if internal:
            # should be searchable but is not needed in API as-is.
            result['url_data'] = url_data

        # Ensure `title` is always non-empty.
        result['title'] = _localize(
            meta.get('title') or
            url_data or
            subject.name
        )

        if subject.source == 'staff':
            if subject.kind == 'user':
                result['link'] = cls.staff_user_link_tpl.format(url_data)
                result['icon'] = cls.staff_user_icon_tpl.format(url_data)
            elif subject.kind == 'group':
                result['link'] = cls._staff_group_url(subject)
                parent = meta.get('parent')
                if parent and isinstance(parent, dict):
                    parent_data = dict(
                        title=_localize(parent.get('title')),
                        link=cls._staff_group_url(DotDict(meta=parent)),
                    )
                    result['parent'] = parent_data
        elif subject.source.startswith('cloud__'):
            # Unlike supposed normal `icon`, this is a link to `avatars.mds.…`,
            # which requires a suffix to be usable.
            result['cloud_icon'] = meta.get('avatar')
            if internal:
                result['__yacloud_ids'] = meta.get('__yacloud_ids')
                result['__yacloud_folder_ids'] = meta.get('__yacloud_folder_ids')
            result['cloud_user_id'] = meta.get('_id')
            result['cloud_user_login'] = meta.get('_login')

        return {key: val for key, val in result.items() if val is not None}

    @classmethod
    def info_to_words_ext(cls, info):
        result = [
            ('name', _sanitize(info.get('name'))),
            ('url_data', _sanitize(info.get('url_data'))),
            ('cloud_user_login', _sanitize(info.get('cloud_user_login'))),
            ('source', '__source__{}'.format(info.get('__source'))),
        ]

        # hax-id for row-level-security, such as user's login.
        rlsid = info.get('__rlsid')
        if rlsid:
            rlsid = rlsid.lower()
            result.extend((
                ('rlsid', rlsid),  # for the actual search.
                ('rlsidpfx', '__rlsid__:{}'.format(rlsid)),
            ))

        result.extend(
            ('yacloud_id', '__yacloud__{}'.format(val))
            for val in info.get('__yacloud_ids') or ()
            if val)
        result.extend(
            ('yacloud_folder_id', '__yacloudfolder__{}'.format(val))
            for val in info.get('__yacloud_folder_ids') or ()
            if val)

        title = info.get('title')
        if isinstance(title, dict):
            result.extend(
                ('title_{}'.format(key), _sanitize(val))
                for key, val in title.items())
        else:
            result.append(('title', _sanitize(title)))
        return list((name, val) for name, val in result if val)

    @classmethod
    def info_to_words(cls, info):
        return list(val for key, val in cls.info_to_words_ext(info))

    # Note: ':' is mostly used for the special cases like `group:yandex`.
    _word_characters = r'\w0-9.:/_-'
    # Extended 'words':
    _word_rex_ex = r'(?u)[{}]+'.format(_word_characters)
    # Minimal 'words':
    _word_rex_min = r'(?u)[\w]+'

    @classmethod
    def process_search_words(cls, words):
        """ list of words -> list of search-internal words """
        result = set()
        for word in words:
            assert isinstance(word, str)
            word = word.lower()
            if ':' in word or word.startswith('__'):
                # Cases:
                # '__rlsid__:...'
                # 'user:b1g...'
                result.add(word)
                continue
            for subword in re.findall(cls._word_rex_ex, word):
                result.add(subword)
            for subword in re.findall(cls._word_rex_min, word):
                result.add(subword)
        return result

    @classmethod
    def preprocess_search_string(cls, string):
        """ user search string -> list of words """
        string = string.lower()
        result = re.findall(cls._word_rex_ex, string)
        result = list(uniq(result))
        return result

    @classmethod
    def search_statement(
            cls, search_string: str, columns=None,
            last_starts=None, last_starts_by_union=True,
            subject_sources=None, extra_words=(),
            active=True, with_all=True,
    ):
        search_words = cls.preprocess_search_string(search_string)
        search_words = list(extra_words) + list(search_words)

        if last_starts is None:
            last_starts = not search_string.endswith(' ')

        Subject = cls.Subject.__table__
        m2m = cls.ss_word_subjects_m2m.__table__
        Word = cls.SSWord.__table__

        # stmt = Subject.select()
        stmt_from = Subject

        stmt_wheres = []
        if active is not None:
            stmt_wheres.append(Subject.c.active == active)
        if subject_sources is not None:
            stmt_wheres.append(Subject.c.source.in_(subject_sources))
        if not with_all:
            stmt_wheres.append(Subject.c.name != 'system_group:all_active_users')
            stmt_wheres.append(Subject.c.name != 'system_group:staff_statleg')

        # last_word_tbl = None
        stmt_wheres_prioritized = None

        for is_last, (idx, search_word) in with_last(enumerate(search_words)):
            m2m_i = m2m.alias('m2m_{}'.format(idx))
            word_i = Word.alias('word_{}'.format(idx))
            stmt_from = stmt_from.join(m2m_i, m2m_i.c.subject_id == Subject.c.id)
            stmt_from = stmt_from.join(word_i, m2m_i.c.ssword_id == word_i.c.id)
            # special case support for 'or'-filtering through 'extra_words'
            if isinstance(search_word, tuple):
                flt = word_i.c.word.in_(search_word)
            else:
                flt = word_i.c.word == search_word

            # if is_last:
            #     last_word_tbl = word_i

            if is_last and last_starts:
                flt_startswith = word_i.c.word.startswith(search_word)
                if last_starts_by_union:
                    # Make a 'prioritized' set of filters with the `equals`,
                    # and use `startswith` for the further results.
                    stmt_wheres_prioritized = stmt_wheres + [flt]
                    flt = flt_startswith
                else:
                    # Use a plain `or` for the
                    flt = flt | flt_startswith

            stmt_wheres += [flt]

        stmt = stmt_from.select(
            # use_labels=True
        ).where(sa_all(stmt_wheres))

        if columns is None:
            columns = [
                Subject.c.id, Subject.c.kind, Subject.c.source,
                Subject.c.name, Subject.c.meta,
                Subject.c.active, Subject.c.search_weight,
            ]
            # if last_word_tbl is not None:
            #     columns += (last_word_tbl.c.word.label('last_word'),)

        if not isinstance(columns, list):
            # TODO: warn (sa 1.4+ does not accept a tuple here)
            columns = list(columns)
        stmt = stmt.with_only_columns(columns)

        if stmt_wheres_prioritized:
            _part_col = lambda val: sa.sql.expression.literal_column(val).label('_part')
            stmt_prioritized = (
                stmt_from.select()
                .where(sa_all(stmt_wheres_prioritized))
                .with_only_columns(columns)
                .column(_part_col('0'))
            )
            stmt = stmt.column(_part_col('1'))
            # `union all` instead of `union` for performance,
            # `distinct on` is for uniqueness of subjects in the result.
            stmt = stmt_prioritized.union_all(stmt)

            # stmt = stmt.order_by('_part')

            # stmt_from_orig = stmt_from
            # stmt_from = stmt

            stmt = stmt.alias('sq_union').select()
            stmt = stmt.distinct('id')
            stmt = stmt.order_by('id', '_part')
            stmt = stmt.alias('sq_distinct').select()
            stmt = stmt.order_by(
                '_part',
                sa.desc('search_weight'),
                'name')
            # stmt = stmt.with_only_columns(['id', 'kind', 'source', 'name', 'meta', 'active'])

            # Subject = stmt_from

        return stmt

    @classmethod
    def search_statement_simple(
            cls, wordsets, columns=None,
            subject_sources=None,
            active=True,
    ):
        """
        :param wordsets: [[word1, word2], [word3]] -> `(word1 or word2) and word3`
        """
        Subject = cls.Subject.__table__
        m2m = cls.ss_word_subjects_m2m.__table__
        Word = cls.SSWord.__table__

        # stmt = Subject.select()
        stmt_from = Subject

        stmt_wheres = []
        if active is not None:
            stmt_wheres.append(Subject.c.active == active)
        if subject_sources is not None:
            stmt_wheres.append(Subject.c.source.in_(subject_sources))

        for idx, wordset in enumerate(wordsets):
            m2m_i = m2m.alias('m2m_{}'.format(idx))
            word_i = Word.alias('word_{}'.format(idx))
            stmt_from = stmt_from.join(m2m_i, m2m_i.c.subject_id == Subject.c.id)
            stmt_from = stmt_from.join(word_i, m2m_i.c.ssword_id == word_i.c.id)
            # special case support for 'or'-filtering through 'extra_words'
            flt = word_i.c.word.in_(wordset)
            stmt_wheres += [flt]

        stmt = stmt_from.select(
            # use_labels=True
        ).where(sa_all(stmt_wheres))

        if columns is None:
            columns = [
                Subject.c.id, Subject.c.kind, Subject.c.source,
                Subject.c.name, Subject.c.meta,
                Subject.c.active, Subject.c.search_weight,
            ]
            # if last_word_tbl is not None:
            #     columns += (last_word_tbl.c.word.label('last_word'),)

        if not isinstance(columns, list):
            # TODO: warn
            columns = list(columns)

        stmt = stmt.with_only_columns(columns)
        return stmt


def main(meth=None):
    if meth is None:
        import sys
        try:
            meth = sys.argv[1]
        except IndexError:
            meth = 'main'
    try:
        from pyaux.runlib import init_logging
        init_logging(level=logging.DEBUG)
    except Exception:  # pylint: disable=broad-except
        logging.basicConfig(level=logging.DEBUG)

    worker = SubjectsManager()
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

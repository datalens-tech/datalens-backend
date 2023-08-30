from __future__ import annotations

import itertools
import logging
import re
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Iterable,
)

import attr
import more_itertools

from bi_api_lib import exc
from bi_api_lib.utils.base import chunks, quote_by_quote, split_by_quoted_quote
from bi_constants.enums import RLSSubjectType
from bi_core.rls import (
    BaseSubjectResolver,
    RLS_ALL_SUBJECT,
    RLS_ALL_SUBJECT_NAME,
    RLS_FAILED_USER_NAME_PREFIX,
    RLS_USERID_SUBJECT,
    RLS_USERID_SUBJECT_NAME,
    RLSEntry,
    RLSPatternType,
    RLSSubject,
)

LOGGER = logging.getLogger(__name__)


class RLSConfigItem(NamedTuple):
    """ Structural item of a minimally parsed RLS config """
    idxes: List[int]  # row numbers
    names: List[str]  # user identifiers


class FieldRLSSerializer:

    allow_all_subject_name: ClassVar[str] = RLS_ALL_SUBJECT_NAME  # `'value': *` cases
    allow_all_subject: ClassVar[RLSSubject] = RLS_ALL_SUBJECT
    userid_subject_name: ClassVar[str] = RLS_USERID_SUBJECT_NAME
    userid_subject: ClassVar[RLSSubject] = RLS_USERID_SUBJECT
    userid_line: ClassVar[str] = 'userid: userid'

    @classmethod
    def to_text_config(cls, data: Iterable[RLSEntry]) -> str:
        # Reminder: this only groups consecutive matching values.
        # Should be good enough for this purpose.
        rls_entries_by_value = itertools.groupby(
            data,
            key=lambda rls_entry: (rls_entry.pattern_type, rls_entry.allowed_value))
        lines = []
        for (pattern_type, allowed_value), rls_entries in rls_entries_by_value:
            subjects_text = ', '.join(
                rls_entry.subject.subject_name
                for rls_entry in rls_entries)
            if pattern_type == RLSPatternType.all:
                line = '*: {}'.format(subjects_text)
            elif pattern_type == RLSPatternType.userid:
                line = cls.userid_line
            elif pattern_type == RLSPatternType.value:
                assert allowed_value is not None
                line = '{}: {}'.format(
                    quote_by_quote(allowed_value),
                    subjects_text)
            else:
                raise Exception("RLS pattern type not yet supported", type(pattern_type), pattern_type)
            lines.append(line)

        return '\n'.join(lines)

    # Subject names list parsing regex.
    # Note that this includes allow_all `*` subjects.
    _subjects_re_s = (
        r'(?P<subjects>'  # name
        r'(?:'  # repeat:
        r'[^,]+'  # subject_name
        r'(?:, *)?'  # `'' or ',' or ', +'`
        r')+'
        r')$'
    )
    # allow_all line `*: user1, user2, …`
    _aa_line_re = re.compile(r"^\s*\*\s*:" + _subjects_re_s)
    _uid_line_re = re.compile(r'\s*'.join(("^", "userid", ":", "userid", "$")))  # 'userid: userid'
    assert _uid_line_re.match(userid_line)  # self-check
    # 'value': user1, user2, …`. Note that the value might contain more quotes.
    _line_re = re.compile(r"^'.+': " + _subjects_re_s)

    @classmethod
    def _parse_single_line(cls, line: str) -> Tuple[RLSPatternType, Optional[str], List[str]]:
        """
        `'value: subjects'` line to `pattern_type, value, subject_names` tuple.
        """
        aa_match = cls._aa_line_re.match(line)

        if aa_match:  # `*: user1, user2, …`
            pattern_type = RLSPatternType.all
            value = None
            subjects_line = aa_match.group('subjects')

        elif cls._uid_line_re.match(line):
            pattern_type = RLSPatternType.userid
            value = None
            subjects_line = 'userid'

        else:  # `'value ''with "quotes"''': user3, user4, …`
            pattern_type = RLSPatternType.value

            if not cls._line_re.match(line):
                raise ValueError('Wrong format')

            value, rest_of_the_line = split_by_quoted_quote(line)
            rest_of_the_line = rest_of_the_line.strip()
            if not rest_of_the_line.startswith(':'):
                raise ValueError(
                    f"Separating ':' expected but not found"
                    f" at {rest_of_the_line[:10]!r}")
            rest_of_the_line = rest_of_the_line[1:].strip()
            subjects_line = rest_of_the_line

        subject_names = [
            name.strip()
            for name in subjects_line.split(',')]

        # Some extra validation.
        if cls.allow_all_subject_name in subject_names:
            if pattern_type == RLSPatternType.all:
                raise ValueError((
                    'Wildcard `*: *` is not allowed.'
                    ' It would effectively disable RLS for the field.'))
            if len(subject_names) != 1:
                # Note that this does not check for
                #     value1: *
                #     value1: user, …
                # lines.
                raise ValueError((
                    'Wildcard user must be the only user in line'
                    ', i.e. `…: *`.'))

        return pattern_type, value, subject_names

    @classmethod
    def _try_parse_single_line(cls, line: str, idx: int) -> Tuple[RLSPatternType, Optional[str], List[str]]:
        try:
            return cls._parse_single_line(line)
        except ValueError as exc_value:
            raise exc.RLSConfigParsingError(
                f'RLS: Parsing failed at line {idx + 1}',
                details=dict(description=str(exc_value)))

    @classmethod
    def _resolve_subject_names(cls, subject_names: List[str], subject_resolver: BaseSubjectResolver) -> Dict[str, Any]:
        """
        Obtain the subject infos from a subject resolver.
        """
        name_to_subject = {}
        # The chunk size is up to tuning,
        # the primary point is to avoid creating too large JSONs,
        # as it gets deserialized in one async iteration.
        # Note that it makes the 'Logins do not exist' error potentially incomplete.
        names_by_account_type = cls._group_names_by_account_type(subject_names)
        for names_chunk in chunks(names_by_account_type.regular_subject_names, 1000):
            subjects = subject_resolver.get_subjects_by_names(names_chunk)
            for subject in subjects:
                # User name without prefix in dict
                name_to_subject[subject.subject_name.removeprefix(RLS_FAILED_USER_NAME_PREFIX)] = subject
        for service_account_str in names_by_account_type.service_account_strs:
            sa_subject: RLSSubject = cls._parse_sa_account_str(service_account_str)
            name_to_subject[sa_subject.subject_name] = sa_subject
        return name_to_subject

    @attr.s
    class AccountGroups:
        regular_subject_names: List[str] = attr.ib()
        service_account_strs: List[str] = attr.ib()  # sa strs have the following format. @sa:{sa_id}

    @classmethod
    def _group_names_by_account_type(cls, subject_names: List[str]) -> AccountGroups:
        regular_subject_names, service_account_strs = more_itertools.partition(cls._is_sa, subject_names)
        return cls.AccountGroups(list(regular_subject_names), list(service_account_strs))

    @classmethod
    def _is_sa(cls, subject_name: str) -> bool:
        return subject_name.startswith('@sa:')

    @classmethod
    def _parse_sa_account_str(cls, sa_str: str) -> RLSSubject:
        # This is a hack. sa names are passed like this -> @sa:{sa_id}
        sa_id = sa_str.removeprefix('@sa:')
        return RLSSubject(
            subject_type=RLSSubjectType.user,
            subject_id=sa_id,
            subject_name=sa_str
        )

    @classmethod
    def pre_parse_text_config(cls, config: str) -> Tuple[RLSConfigItem, RLSConfigItem, Dict[str, RLSConfigItem]]:
        """
        Parse the text config into a
        `(allow_all: RLSConfigItem, {field_value: RLSConfigItem, ...})`
        pair.

        `allow_all` here is `*: subject1, …` cases, not to be confused with
        wildcard subjects.
        """
        allow_all_item = RLSConfigItem(idxes=[], names=[])
        userid_item = RLSConfigItem(idxes=[], names=[])
        value_to_item: Dict[str, RLSConfigItem] = {}

        if not config:
            return allow_all_item, userid_item, value_to_item

        for idx, line in enumerate(config.strip().split('\n')):
            pattern_type, value, subject_names = cls._try_parse_single_line(
                line=line, idx=idx)

            if pattern_type == RLSPatternType.all:
                assert not value
                value_item = allow_all_item
            elif pattern_type == RLSPatternType.userid:
                assert not value
                assert subject_names == ['userid']
                value_item = userid_item
            elif pattern_type == RLSPatternType.value:
                assert value is not None
                maybe_value_item = value_to_item.get(value)
                if maybe_value_item is not None:
                    value_item = maybe_value_item
                else:
                    value_item = RLSConfigItem(idxes=[], names=[])
                    value_to_item[value] = value_item
            else:
                raise Exception("RLS pattern type is not supported", type(pattern_type), pattern_type)

            value_item.idxes.append(idx)
            # remove failed user name prefix
            value_item.names.extend([name.removeprefix(RLS_FAILED_USER_NAME_PREFIX) for name in subject_names])

        return allow_all_item, userid_item, value_to_item

    @classmethod
    def from_text_config(cls, config: str, field_guid: str, subject_resolver: Optional[BaseSubjectResolver]) -> List[RLSEntry]:
        if not config:
            return []

        allow_all_item, userid_item, value_to_item = cls.pre_parse_text_config(config)

        # TODO: check for
        #     value1: *
        #     value1: user1, …
        # lines.

        # TODO: check for
        #     *: user1
        #     value: user1
        # lines.

        all_names = set(
            name
            for value_item in itertools.chain(
                value_to_item.values(),
                [allow_all_item])  # wildcard value
            for name in value_item.names)
        all_names -= {cls.allow_all_subject_name}  # wildcard subject
        all_names_lst = sorted(all_names)

        if subject_resolver is not None:
            name_to_subject = cls._resolve_subject_names(all_names_lst, subject_resolver=subject_resolver)
        else:
            name_to_subject = {
                name: RLSSubject(
                    subject_type=RLSSubjectType.unknown,
                    subject_id='',
                    subject_name=name,
                )
                for name in all_names_lst}
        # `resolve_subjects`-independent hack
        name_to_subject[cls.allow_all_subject_name] = cls.allow_all_subject

        # Combine the results.
        rls_entries = []
        for value, value_info in value_to_item.items():
            names = sorted(set(value_info.names))
            for name in names:
                # TODO?: write down the source config line idx too.
                rls_entries.append(RLSEntry(
                    field_guid=field_guid,
                    allowed_value=value,
                    subject=name_to_subject[name],
                    pattern_type=RLSPatternType.value,
                ))
        for name in allow_all_item.names:
            rls_entries.append(RLSEntry(
                field_guid=field_guid,
                allowed_value=None,
                subject=name_to_subject[name],
                pattern_type=RLSPatternType.all,
            ))
        for name in userid_item.names:
            assert name == cls.userid_subject_name
            rls_entries.append(RLSEntry(
                field_guid=field_guid,
                allowed_value=None,
                subject=cls.userid_subject,
                pattern_type=RLSPatternType.userid,
            ))

        return rls_entries

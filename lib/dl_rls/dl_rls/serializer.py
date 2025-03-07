from __future__ import annotations

import itertools
import logging
import re
from typing import (
    ClassVar,
    Iterable,
    NamedTuple,
    Optional,
)

from dl_constants.enums import RLSSubjectType
from dl_rls import exc
from dl_rls.models import (
    RLS_FAILED_USER_NAME_PREFIX,
    RLS2ConfigEntry,
    RLS2Subject,
    RLSEntry,
    RLSPatternType,
    RLSSubject,
)
from dl_rls.subject_resolver import BaseSubjectResolver
from dl_rls.utils import (
    chunks,
    quote_by_quote,
    split_by_quoted_quote,
)


LOGGER = logging.getLogger(__name__)


class RLSConfigItem(NamedTuple):
    """Structural item of a minimally parsed RLS config"""

    idxes: list[int]  # row numbers
    names: list[str]  # user identifiers


class FieldRLSSerializer:
    allow_all_subject_name: ClassVar[str] = "*"  # `'value': *` cases
    allow_all_subject: ClassVar[RLSSubject] = RLSSubject(
        subject_type=RLSSubjectType.all, subject_id="*", subject_name="*"
    )
    userid_subject_name: ClassVar[str] = "userid"
    userid_subject: ClassVar[RLSSubject] = RLSSubject(
        subject_type=RLSSubjectType.userid, subject_id="", subject_name="userid"
    )
    userid_line: ClassVar[str] = "userid: userid"

    sa_prefix: ClassVar[str] = "@sa:"
    group_prefix: ClassVar[str] = "@group:"

    @classmethod
    def to_text_config(cls, data: Iterable[RLSEntry]) -> str:
        # Reminder: this only groups consecutive matching values.
        # Should be good enough for this purpose.
        rls_entries_by_value = itertools.groupby(
            data, key=lambda rls_entry: (rls_entry.pattern_type, rls_entry.allowed_value)
        )
        lines = []
        for (pattern_type, allowed_value), rls_entries in rls_entries_by_value:
            subjects_text = ", ".join(rls_entry.subject.subject_name for rls_entry in rls_entries)
            if pattern_type == RLSPatternType.all:
                line = "*: {}".format(subjects_text)
            elif pattern_type == RLSPatternType.userid:
                line = cls.userid_line
            elif pattern_type == RLSPatternType.value:
                assert allowed_value is not None
                line = "{}: {}".format(quote_by_quote(allowed_value), subjects_text)
            else:
                raise Exception("RLS pattern type not yet supported", type(pattern_type), pattern_type)
            lines.append(line)

        return "\n".join(lines)

    # Subject names list parsing regex.
    # Note that this includes allow_all `*` subjects.
    _subjects_re_s = (
        r"(?P<subjects>"  # name
        r"(?:"  # repeat:
        r"[^,]+"  # subject_name
        r"(?:, *)?"  # `'' or ',' or ', +'`
        r")+"
        r")$"
    )
    # allow_all line `*: user1, user2, …`
    _aa_line_re = re.compile(r"^\s*\*\s*:" + _subjects_re_s)
    _uid_line_re = re.compile(r"\s*".join(("^", "userid", ":", "userid", "$")))  # 'userid: userid'
    assert _uid_line_re.match(userid_line)  # self-check
    # 'value': user1, user2, …`. Note that the value might contain more quotes.
    _line_re = re.compile(r"^'.+': " + _subjects_re_s)

    @classmethod
    def _parse_single_line(cls, line: str) -> tuple[RLSPatternType, Optional[str], list[str]]:
        """
        `'value: subjects'` line to `pattern_type, value, subject_names` tuple.
        """
        aa_match = cls._aa_line_re.match(line)

        if aa_match:  # `*: user1, user2, …`
            pattern_type = RLSPatternType.all
            value = None
            subjects_line = aa_match.group("subjects")

        elif cls._uid_line_re.match(line):
            pattern_type = RLSPatternType.userid
            value = None
            subjects_line = "userid"

        else:  # `'value ''with "quotes"''': user3, user4, …`
            pattern_type = RLSPatternType.value

            if not cls._line_re.match(line):
                raise ValueError("Wrong format")

            value, rest_of_the_line = split_by_quoted_quote(line)
            rest_of_the_line = rest_of_the_line.strip()
            if not rest_of_the_line.startswith(":"):
                raise ValueError(f"Separating ':' expected but not found" f" at {rest_of_the_line[:10]!r}")
            rest_of_the_line = rest_of_the_line[1:].strip()
            subjects_line = rest_of_the_line

        subject_names = [name.strip() for name in subjects_line.split(",")]

        # Some extra validation.
        if cls.allow_all_subject_name in subject_names:
            if pattern_type == RLSPatternType.all:
                raise ValueError("Wildcard `*: *` is not allowed. It would effectively disable RLS for the field.")
            if len(subject_names) != 1:
                # Note that this does not check for
                #     value1: *
                #     value1: user, …
                # lines.
                raise ValueError("Wildcard user must be the only user in line, i.e. `…: *`.")

        return pattern_type, value, subject_names

    @classmethod
    def _try_parse_single_line(cls, line: str, idx: int) -> tuple[RLSPatternType, Optional[str], list[str]]:
        try:
            return cls._parse_single_line(line)
        except ValueError as exc_value:
            raise exc.RLSConfigParsingError(
                f"RLS: Parsing failed at line {idx + 1}", details=dict(description=str(exc_value))
            ) from exc_value

    class AccountGroups(NamedTuple):
        users: list[str]  # {user_id} (without any prefixes)
        service_accounts: list[str]  # @sa:{sa_id}
        groups: list[str]  # @group:{group_id}

    @classmethod
    def _group_subject_names_by_type(cls, subject_names: list[str]) -> AccountGroups:
        users, service_accounts, groups = [], [], []
        for name in subject_names:
            if name.startswith(cls.sa_prefix):
                service_accounts.append(name)
            elif name.startswith(cls.group_prefix):
                groups.append(name)
            else:
                users.append(name)
        return cls.AccountGroups(users, service_accounts, groups)

    @classmethod
    def _parse_sa_str(cls, sa_str: str) -> RLSSubject:
        sa_id = sa_str.removeprefix(cls.sa_prefix)
        return RLSSubject(subject_type=RLSSubjectType.user, subject_id=sa_id, subject_name=sa_str)

    @classmethod
    def _parse_group_str(cls, group_str: str) -> RLSSubject:
        group_id = group_str.removeprefix(cls.group_prefix)
        return RLSSubject(subject_type=RLSSubjectType.group, subject_id=group_id, subject_name=group_str)

    @classmethod
    def _resolve_subject_names(
        cls, subject_names: list[str], subject_resolver: BaseSubjectResolver
    ) -> dict[str, RLSSubject]:
        """
        Obtain the subject infos from a subject resolver.
        """
        name_to_subject: dict[str, RLSSubject] = {}
        # The chunk size is up to tuning,
        # the primary point is to avoid creating too large JSONs,
        # as it gets deserialized in one async iteration.
        # Note that it makes the 'Logins do not exist' error potentially incomplete.
        users, service_accounts, groups = cls._group_subject_names_by_type(subject_names)
        for names_chunk in chunks(users, 1000):
            subjects = subject_resolver.get_subjects_by_names(names_chunk)
            for subject in subjects:
                # User name without prefix in dict
                name_to_subject[subject.subject_name.removeprefix(RLS_FAILED_USER_NAME_PREFIX)] = subject
        for sa_str in service_accounts:
            sa_subject: RLSSubject = cls._parse_sa_str(sa_str)
            name_to_subject[sa_subject.subject_name] = sa_subject
        for group_str in groups:
            group_subject: RLSSubject = cls._parse_group_str(group_str)
            name_to_subject[group_subject.subject_name] = group_subject
        return name_to_subject

    @classmethod
    def pre_parse_text_config(cls, config: str) -> tuple[RLSConfigItem, RLSConfigItem, dict[str, RLSConfigItem]]:
        """
        Parse the text config into a
        `(allow_all: RLSConfigItem, {field_value: RLSConfigItem, ...})`
        pair.

        `allow_all` here is `*: subject1, …` cases, not to be confused with
        wildcard subjects.
        """
        allow_all_item = RLSConfigItem(idxes=[], names=[])
        userid_item = RLSConfigItem(idxes=[], names=[])
        value_to_item: dict[str, RLSConfigItem] = {}

        if not config:
            return allow_all_item, userid_item, value_to_item

        for idx, line in enumerate(config.strip().split("\n")):
            pattern_type, value, subject_names = cls._try_parse_single_line(line=line, idx=idx)

            if pattern_type == RLSPatternType.all:
                assert not value
                value_item = allow_all_item
            elif pattern_type == RLSPatternType.userid:
                assert not value
                assert subject_names == ["userid"]
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
    def from_text_config(
        cls, config: str, field_guid: str, subject_resolver: Optional[BaseSubjectResolver]
    ) -> list[RLSEntry]:
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
            for value_item in itertools.chain(value_to_item.values(), [allow_all_item])  # wildcard value
            for name in value_item.names
        )
        all_names -= {cls.allow_all_subject_name}  # wildcard subject
        all_names_lst = sorted(all_names)

        if subject_resolver is not None:
            name_to_subject = cls._resolve_subject_names(all_names_lst, subject_resolver=subject_resolver)
        else:
            name_to_subject = {
                name: RLSSubject(subject_type=RLSSubjectType.unknown, subject_id="", subject_name=name)
                for name in all_names_lst
            }
        # `resolve_subjects`-independent hack
        name_to_subject[cls.allow_all_subject_name] = cls.allow_all_subject

        # Combine the results.
        rls_entries = []
        for value, value_info in value_to_item.items():
            names = sorted(set(value_info.names))
            for name in names:
                # TODO?: write down the source config line idx too.
                rls_entries.append(
                    RLSEntry(
                        field_guid=field_guid,
                        allowed_value=value,
                        subject=name_to_subject[name],
                        pattern_type=RLSPatternType.value,
                    )
                )
        for name in allow_all_item.names:
            rls_entries.append(
                RLSEntry(
                    field_guid=field_guid,
                    allowed_value=None,
                    subject=name_to_subject[name],
                    pattern_type=RLSPatternType.all,
                )
            )
        for name in userid_item.names:
            assert name == cls.userid_subject_name
            rls_entries.append(
                RLSEntry(
                    field_guid=field_guid,
                    allowed_value=None,
                    subject=cls.userid_subject,
                    pattern_type=RLSPatternType.userid,
                )
            )

        return rls_entries

    @classmethod
    def to_v2_config(cls, data: list[RLSEntry]) -> list[RLS2ConfigEntry]:
        return [
            RLS2ConfigEntry(
                field_guid=entry.field_guid,
                pattern_type=entry.pattern_type,
                allowed_value=entry.allowed_value,
                subject=RLS2Subject(
                    subject_id=entry.subject.subject_id,
                    subject_type=entry.subject.subject_type,
                    subject_name=entry.subject.subject_name,
                ),
            )
            for entry in data
        ]

    @classmethod
    def from_v2_config(cls, data: list[RLS2ConfigEntry], field_guid: str) -> list[RLSEntry]:
        rls_entries = []
        for entry in data:
            if entry.pattern_type == RLSPatternType.value and entry.allowed_value is None:
                raise Exception("allowed_value must be not None for value RLS pattern type")
            if entry.pattern_type != RLSPatternType.value and entry.allowed_value is not None:
                raise Exception(f"allowed_value must be None for {entry.pattern_type.value} RLS pattern type")
            rls_entries.append(
                RLSEntry(
                    field_guid=field_guid,
                    pattern_type=entry.pattern_type,
                    allowed_value=entry.allowed_value,
                    subject=RLSSubject(
                        subject_type=entry.subject.subject_type,
                        subject_id=entry.subject.subject_id,
                        subject_name=entry.subject.subject_name or entry.subject.subject_id,
                    ),
                )
            )
        return rls_entries

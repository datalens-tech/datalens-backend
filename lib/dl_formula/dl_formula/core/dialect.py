""" ... """

from __future__ import annotations

from functools import reduce
import inspect
from itertools import chain
import logging
from operator import ior
import re
from typing import (
    Any,
    Collection,
    Generator,
    NamedTuple,
    Optional,
    Type,
)

from dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)


LOGGER = logging.getLogger(__name__)


VersionType = tuple[int, ...]

_NO_VERSION: VersionType = ()


class DialectName(DynamicEnum):
    UNKNOWN = AutoEnumValue()
    DUMMY = AutoEnumValue()
    ANY = AutoEnumValue()
    SQLITE = AutoEnumValue()  # TODO: Remove


class DialectBit(NamedTuple):
    name: DialectName
    version: VersionType = ()

    @property
    def orderable(self) -> tuple[str, VersionType]:
        return self.name.value, self.version


class DialectCombo(NamedTuple):
    bits: frozenset[DialectBit]

    @property
    def ambiguity(self) -> int:
        return len(self.bits)

    @property
    def deterministic(self) -> bool:
        return self.ambiguity == 1

    @property
    def single_bit(self) -> DialectBit:
        assert self.deterministic
        return next(iter(self.bits))

    @property
    def common_name(self) -> DialectName:
        names = {bit.name for bit in self.bits}
        assert len(names) == 1, str(names)
        return next(iter(names))

    @property
    def common_version(self) -> VersionType:
        versions = {bit.version for bit in self.bits}
        assert len(versions) == 1, str(versions)
        return next(iter(versions))

    @property
    def common_name_and_version(self) -> str:
        return f'{self.common_name.name}_{"_".join((str(part) for part in self.common_version))}'.strip("_")

    def __sub__(self, other: Any) -> DialectCombo:
        if isinstance(other, DialectBit):
            return DialectCombo(
                bits=self.bits - frozenset((other,)),
            )
        if isinstance(other, DialectCombo):
            return DialectCombo(
                bits=self.bits - other.bits,
            )
        else:
            raise TypeError(type(other))

    def __or__(self, other: Any) -> DialectCombo:
        if isinstance(other, DialectBit):
            return DialectCombo(
                bits=self.bits | frozenset((other,)),
            )
        if isinstance(other, DialectCombo):
            return DialectCombo(
                bits=self.bits | other.bits,
            )
        else:
            raise TypeError(type(other))

    def __and__(self, other: Any) -> DialectCombo:
        if isinstance(other, DialectBit):
            return DialectCombo(
                bits=self.bits & frozenset((other,)),
            )
        if isinstance(other, DialectCombo):
            return DialectCombo(
                bits=self.bits & other.bits,
            )
        else:
            raise TypeError(type(other))

    def __bool__(self) -> bool:
        return bool(self.bits)

    def to_list(self, with_self: bool = False) -> list[DialectCombo]:
        result = [DialectCombo(bits=frozenset((bit,))) for bit in self.bits]
        result.sort(key=lambda dialect: (dialect.single_bit.name.value, dialect.single_bit.version))
        if with_self and not self.deterministic:
            result.append(self)
        return result


def simple_combo(name: DialectName, version: VersionType = ()) -> DialectCombo:
    return DialectCombo(bits=frozenset((DialectBit(name=name, version=version),)))


class DialectNamespace:
    @classmethod
    def by_name(cls, item: str) -> DialectCombo:
        assert isinstance(item, str)
        return getattr(cls, item)

    @classmethod
    def iter_named_combos(cls) -> Generator[tuple[str, DialectCombo], None, None]:
        for name, item in inspect.getmembers(cls):
            if isinstance(item, DialectCombo):
                yield name, item

    @classmethod
    def all_iter(cls) -> Generator[DialectCombo, None, None]:
        for _, item in inspect.getmembers(cls):
            if isinstance(item, DialectCombo):
                yield item

    @classmethod
    def all_basic_iter(cls) -> Generator[DialectCombo, None, None]:
        for item in cls.all_iter():
            if item.deterministic:
                yield item

    @classmethod
    def all(cls) -> list[DialectCombo]:
        return list(cls.all_iter())

    @classmethod
    def and_above(cls, dialect: DialectCombo) -> DialectCombo:
        if not dialect.deterministic:
            raise ValueError("Only deterministic dialects are allowed")
        bit = dialect.single_bit
        return DialectCombo(
            bits=frozenset(
                item.single_bit
                for item in cls.all_basic_iter()
                if (item.single_bit.name == bit.name and item.single_bit.version >= bit.version)
            )
        )

    @classmethod
    def and_below(cls, dialect: DialectCombo) -> DialectCombo:
        if not dialect.deterministic:
            raise ValueError("Only deterministic dialects are allowed")
        bit = dialect.single_bit
        return DialectCombo(
            bits=frozenset(
                item.single_bit
                for item in cls.all_basic_iter()
                if (item.single_bit.name == bit.name and () < item.single_bit.version <= bit.version)
            )
        )


class StandardDialect(DialectNamespace):
    # Generic
    EMPTY = DialectCombo(bits=frozenset())
    DUMMY = simple_combo(name=DialectName.DUMMY)
    ANY = simple_combo(name=DialectName.ANY)

    # Others
    SQLITE = simple_combo(name=DialectName.SQLITE)


dialect_defaults: dict[DialectName, DialectCombo] = {
    DialectName.UNKNOWN: StandardDialect.EMPTY,
    DialectName.DUMMY: StandardDialect.DUMMY,
    DialectName.SQLITE: StandardDialect.SQLITE,
}


def register_default_dialect(dialect_name: DialectName, dialect_combo: DialectCombo) -> None:
    if dialect_name in dialect_defaults:
        assert dialect_defaults[dialect_name] is dialect_combo
    else:
        dialect_defaults[dialect_name] = dialect_combo


def from_name_and_version(
    dialect_name: DialectName,
    dialect_version: Optional[str],
) -> DialectCombo:
    """Return ``DialectCombo`` instance corresponding to the version string (``SELECT VERSION()``)"""

    parsed_version: Optional[VersionType]
    if dialect_version:
        version_match = re.search(r"(?P<version>\d+(\.\d+)+)", dialect_version)
        if version_match is None:
            parsed_version = None
        else:
            raw_version = version_match.group("version")
            parsed_version = tuple(int(part) for part in raw_version.split("."))
    else:
        parsed_version = None

    LOGGER.info(f"Parsed dialect version {dialect_version} as {parsed_version}")

    if parsed_version is None and dialect_name in dialect_defaults:
        LOGGER.info(f"Falling back to {dialect_defaults[dialect_name]}")
        return dialect_defaults[dialect_name]

    matches_by_name = sorted(
        chain.from_iterable(
            [
                item.to_list()
                for item in _ALL_BASIC_DIALECTS
                if item.deterministic and item.single_bit.name == dialect_name
            ],
        ),
        key=lambda item: item.single_bit.version,
    )
    matches_by_version = [
        item for item in matches_by_name if parsed_version is None or item.single_bit.version <= parsed_version
    ]
    if matches_by_version:
        # perfect match. Return the latest dialect version possible
        matched_dialect = matches_by_version[-1]
        LOGGER.info(f"Matched dialect found: {matched_dialect.single_bit}")
        return matched_dialect

    LOGGER.warning('Failed to get dialect for DB "%s" and version "%s"', dialect_name.name, dialect_version)
    if matches_by_name:
        # no matching dialect found. Fall back to the earliest supported for this DB type
        LOGGER.warning("Falling back to `%s`", matches_by_name[0].single_bit.name.name)
        return matches_by_name[0]

    # nothing. Just nothing
    raise ValueError("Invalid dialect parameters")


_ALL_BASIC_DIALECTS: set[DialectCombo] = {
    StandardDialect.DUMMY,
}


def register_basic_dialects(dialect: DialectCombo) -> None:
    for basic_dialect in dialect.to_list():
        _ALL_BASIC_DIALECTS.add(basic_dialect)


def get_all_basic_dialects() -> list[DialectCombo]:
    return sorted(_ALL_BASIC_DIALECTS, key=lambda x: x.single_bit.orderable)


_NAMED_DIALECT_COMBOS: dict[str, DialectCombo] = {}


def register_dialect_namespace(dialect_ns: Type[DialectNamespace]) -> None:
    for name, combo in dialect_ns.iter_named_combos():
        try:
            assert _NAMED_DIALECT_COMBOS[name] == combo
        except KeyError:
            _NAMED_DIALECT_COMBOS[name] = combo


def dialect_combo_is_supported(supported: DialectCombo | Collection[DialectCombo], current: DialectCombo) -> bool:
    supported_dcombo: DialectCombo
    if not isinstance(supported, DialectCombo):
        supported_dcombo = reduce(ior, supported)  # bitwise "or" (|) of the collection's elements
    else:
        supported_dcombo = supported

    if supported_dcombo & StandardDialect.ANY:
        return True

    return supported_dcombo & current == current


def get_dialect_combos() -> list[DialectCombo]:
    return list(_NAMED_DIALECT_COMBOS.values())


def get_dialect_from_str(dialect_str: str) -> DialectCombo:
    return _NAMED_DIALECT_COMBOS[dialect_str]


register_dialect_namespace(StandardDialect)

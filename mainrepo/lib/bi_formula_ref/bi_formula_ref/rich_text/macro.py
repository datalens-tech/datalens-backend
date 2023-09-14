from typing import (
    List,
    Optional,
)

import attr


@attr.s
class BaseMacro:
    pass


@attr.s
class SingleArgMacro(BaseMacro):
    arg: str = attr.ib(kw_only=True)


@attr.s
class DoubleArgMacro(SingleArgMacro):
    second_arg: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s
class ListMacro(BaseMacro):
    values: List[str] = attr.ib(kw_only=True)


class RefMacro(DoubleArgMacro):
    category_name: Optional[str] = None


class LinkMacro(DoubleArgMacro):
    pass


class TableMacro(SingleArgMacro):
    pass


class TextMacro(SingleArgMacro):
    pass


class DialectsMacro(ListMacro):
    pass


class ArgMacro(SingleArgMacro):
    pass


class ArgNMacro(SingleArgMacro):
    pass


class TypeMacro(ListMacro):
    pass


class ExtMacroMacro(SingleArgMacro):
    pass


class CategoryMacro(DoubleArgMacro):
    anchor_name: Optional[str] = None


SINGLE_ARG_MACROS = {
    "table": TableMacro,
    "text": TextMacro,
    "arg": ArgMacro,
    "argn": ArgNMacro,
    "macro": ExtMacroMacro,
}
DOUBLE_ARG_MACROS = {
    "ref": RefMacro,
    "link": LinkMacro,
    "category": CategoryMacro,
}
LIST_MACROS = {
    "dialects": DialectsMacro,
    "type": TypeMacro,
}

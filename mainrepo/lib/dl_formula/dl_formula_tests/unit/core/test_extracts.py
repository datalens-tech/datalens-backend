from __future__ import annotations

from dl_formula.parser.factory import get_parser


def test_extract():
    parser = get_parser()
    assert parser.parse('my_func(  123, "qwerty")').extract == parser.parse("MY_FUNC(123, 'qwerty')").extract
    assert parser.parse("MY_FUNC(123, 'qwerty')").extract != parser.parse("MY_FUNC(1, 'qwerty')").extract
    assert parser.parse("named_without_children()").extract != parser.parse("[named_without_children]").extract

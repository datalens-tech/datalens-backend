from __future__ import annotations

from typing import (
    Any,
    ClassVar,
)

import pytest
import sqlalchemy as sa

from bi_formula_testing.evaluator import DbEvaluator
from bi_formula_testing.testcases.base import FormulaConnectorTestBase
from bi_formula_testing.util import (
    to_str,
    to_unicode,
)

CONTAINS_TESTS = [
    ('"a b c % d e"', '""', True),
    ('"a b c % d e"', '"c % d"', True),
    ('"a b c % d e"', '"c \\\\% d"', False),
    ('"a b c % d e"', '"b \\\\% d"', False),
    ('"a b c %% d e"', '""', True),
    ('"a b c %% d e"', '"c % d"', False),
    ('"a b c %% d e"', '"c %% d"', True),
    ('"a b c %% d e"', '"c \\\\% d"', False),
    ('"a b c %% d e"', '"b \\\\% d"', False),
    ('"a b c \\\\% d e"', '""', True),
    ('"a b c \\\\% d e"', '"c % d"', False),
    ('"a b c \\\\% d e"', '"c \\\\% d"', True),
    ('"a b c \\\\% d e"', '"b \\\\% d"', False),
    ('"a b c \\\\\\\\% d e"', '""', True),
    ('"a b c \\\\\\\\% d e"', '"c % d"', False),
    ('"a b c \\\\\\\\% d e"', '"c \\\\% d"', False),
    ('"a b c \\\\\\\\% d e"', '"c \\\\\\\\% d"', True),
    ('"a b c \\\\\\\\% d e"', '"b \\\\% d"', False),
]


class DefaultStringFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    datetime_str_separator: ClassVar[str] = " "
    datetime_str_ending: ClassVar[str] = ""
    supports_trimming_funcs: ClassVar[bool] = True
    supports_regex_extract: ClassVar[bool] = True
    supports_regex_extract_nth: ClassVar[bool] = True
    supports_regex_replace: ClassVar[bool] = True
    supports_regex_match: ClassVar[bool] = True
    supports_split_3: ClassVar[bool] = True
    supports_non_const_percent_escape: ClassVar[bool] = True

    def test_char_conversions(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('ASCII("W")') == 87
        assert to_str(dbe.eval("CHAR(87)")) == "W"

    def test_concat(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert to_str(dbe.eval('CONCAT("abc", "def", "qwe")')) == "abcdefqwe"
        sep = self.datetime_str_separator
        end = self.datetime_str_ending
        exp_value = f"1abcFalse2019-01-012019-01-01{sep}01:02:03{end}"
        assert to_str(dbe.eval('CONCAT(1, "abc", FALSE, #2019-01-01#, #2019-01-01 01:02:03#)')) == exp_value
        # const str
        assert to_str(dbe.eval('CONCAT("abc", "def")')) == "abcdef"

        # Single argument case
        # const
        assert to_str(dbe.eval('CONCAT("abc")')) == "abc"
        # non-const (different implementation)
        assert to_str(dbe.eval("CONCAT([str_value])", from_=data_table, order_by=["[str_value]"])) == "q"
        assert to_str(dbe.eval("CONCAT([int_value])", from_=data_table, order_by=["[int_value]"])) == "0"

    def test_find(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('FIND("Lorem ipsum dolor sit amet", "psu")') == 8
        assert dbe.eval('FIND("Lorem ipsum dolor sit amet", "abc")') == 0
        assert dbe.eval('FIND("Карл у Клары украл кораллы", "рал")') == 16
        assert dbe.eval('FIND("Lorem ipsum dolor sit amet", "or", 7)') == 16
        assert dbe.eval('FIND("Карл у Клары украл кораллы", "рал", 18)') == 22

    def test_left_right(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('LEFT("Lorem ipsum", 4)')) == "Lore"
        assert to_str(dbe.eval('LEFT("Карл у Клары", 4)')) == "Карл"

        assert to_str(dbe.eval('RIGHT("Lorem ipsum", 4)')) == "psum"
        assert to_str(dbe.eval('RIGHT("Карл у Клары", 4)')) == "лары"

    def test_len(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('LEN("Lorem ipsum")') == 11
        assert dbe.eval('LEN("Карл у Клары")') == 12

    def test_lower_upper(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert to_str(dbe.eval('LOWER("Lorem ipsum")')) == "lorem ipsum"
        assert to_str(dbe.eval('LOWER(__LIT__("Lorem ipsum"))')) == "lorem ipsum"
        assert to_unicode(dbe.eval('LOWER("Карл у Клары")')) == "карл у клары"
        assert to_unicode(dbe.eval('LOWER(__LIT__("Карл у Клары"))')) == "карл у клары"

        assert to_str(dbe.eval('UPPER("Lorem ipsum")')) == "LOREM IPSUM"
        assert to_str(dbe.eval('UPPER(__LIT__("Lorem ipsum"))')) == "LOREM IPSUM"
        assert to_unicode(dbe.eval('UPPER("Карл у Клары")')) == "КАРЛ У КЛАРЫ"
        assert to_unicode(dbe.eval('UPPER(__LIT__("Карл у Клары"))')) == "КАРЛ У КЛАРЫ"

    def test_trimming(self, dbe: DbEvaluator) -> None:
        if not self.supports_trimming_funcs:
            pytest.skip()

        assert to_str(dbe.eval('LTRIM("  Lorem ipsum   ")')) == "Lorem ipsum   "
        assert to_str(dbe.eval('LTRIM(" \\n Lorem ipsum   ")')) == "\n Lorem ipsum   "

        assert to_str(dbe.eval('RTRIM("  Lorem ipsum   ")')) == "  Lorem ipsum"
        assert to_str(dbe.eval('RTRIM("  Lorem ipsum  \\n ")')) == "  Lorem ipsum  \n"

        assert to_str(dbe.eval('TRIM("  Lorem ipsum   ")')) == "Lorem ipsum"
        assert to_str(dbe.eval('TRIM(" \\n Lorem ipsum   ")')) == "\n Lorem ipsum"

    def test_substr(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('SUBSTR("Lorem ipsum", 3)')) == "rem ipsum"
        assert to_str(dbe.eval('SUBSTR("Карл у Клары", 3)')) == "рл у Клары"
        assert to_str(dbe.eval('SUBSTR("Lorem ipsum", 3, 6)')) == "rem ip"
        assert to_str(dbe.eval('SUBSTR("Карл у Клары", 3, 6)')) == "рл у К"

    def test_regexp_extract(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_regex_extract:
            pytest.skip()

        assert to_str(dbe.eval('REGEXP_EXTRACT("Lorem ipsum dolor sit amet", "or..")')) == "orem"
        assert to_str(dbe.eval('REGEXP_EXTRACT("Lorem ipsum dolor sit amet", "abc")')) in (None, "")
        assert to_str(dbe.eval('REGEXP_EXTRACT("Карл у Клары украл кораллы", ".лары")')) == "Клары"
        assert to_str(dbe.eval('REGEXP_EXTRACT([str_null_value], "or..")', from_=data_table)) in (None, "")

    def test_regexp_extract_nth(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_regex_extract_nth:
            pytest.skip()

        assert to_str(dbe.eval('REGEXP_EXTRACT_NTH("Lorem ipsum dolor sit amet", "or..", 2)')) == "or s"
        assert to_str(dbe.eval('REGEXP_EXTRACT_NTH("Карл у Клары украл кораллы", ".рал", 2)')) == "орал"
        assert to_str(dbe.eval('REGEXP_EXTRACT_NTH([str_null_value], "or..", 2)', from_=data_table)) in (None, "")

    def test_regexp_extract_replace(self, dbe: DbEvaluator) -> None:
        if not self.supports_regex_replace:
            pytest.skip()

        assert to_str(dbe.eval('REGEXP_REPLACE("Lorem ipsum dolor sit amet", "or..", "____")')) == (
            "L____ ipsum dol____it amet"
        )
        assert to_str(dbe.eval('REGEXP_REPLACE("Карл у Клары украл кораллы", "рал.", "роп")')) == (
            "Карл у Клары укропкоропы"
        )

    def test_regexp_match(self, dbe: DbEvaluator) -> None:
        if not self.supports_regex_match:
            pytest.skip()

        assert dbe.eval('REGEXP_MATCH("Lorem ipsum", ".*ips.*")')
        assert not dbe.eval('REGEXP_MATCH("Lorem ipsum", ".*ipss.*")')
        assert dbe.eval('REGEXP_MATCH("Карл у Клары", ".*арл.*")')
        assert not dbe.eval('REGEXP_MATCH("Карл у Клары", ".*марл.*")')

    def test_replace(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('REPLACE("Lorem ipsum dolor sit amet", "or", "__")')) == "L__em ipsum dol__ sit amet"
        assert to_str(dbe.eval('REPLACE("Карл у Клары украл кораллы", "ра", "ар")')) == "Карл у Клары укарл коарллы"

    def test_space(self, dbe: DbEvaluator) -> None:
        assert (
            to_str(dbe.eval("SPACE(4)")) == "    "
        )  # SPACE(<int literal>) is evaluated in Python rather than in the DB,
        assert to_str(dbe.eval("SPACE(2 + 2)")) == "    "  # while this version actually uses the DB function(s)

    def test_split_3(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_split_3:
            pytest.skip()

        assert to_str(dbe.eval('SPLIT("Lorem ipsum dolor sit amet", " ", 3)')) == "dolor"
        assert to_str(dbe.eval('SPLIT([str_null_value], " ", 3)', from_=data_table)) in (None, "")

    def test_split_1_2(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_arrays:
            pytest.skip()

        assert dbe.eval('ARR_STR(SPLIT(",One,two,three,"), "+")', from_=data_table) == "+One+two+three+"
        assert dbe.eval('ARR_STR(SPLIT(";One;two,three,", ";"), "+")', from_=data_table) == "+One+two,three,"
        assert dbe.eval('ARR_STR(SPLIT(""))', from_=data_table) == ""
        assert dbe.eval('ARR_STR(SPLIT("", "@"))', from_=data_table) == ""

    def test_contains_simple(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('CONTAINS("Lorem ipsum", "ips")')
        assert not dbe.eval('CONTAINS("Lorem ipsum", "abc")')
        assert dbe.eval('CONTAINS(__LIT__("Lorem ipsum"), __LIT__("ips"))')
        assert not dbe.eval('CONTAINS(__LIT__("Lorem ipsum"), __LIT__("abc"))')
        assert dbe.eval('CONTAINS("Lorem %ipsum", "em %ip")')
        assert dbe.eval('CONTAINS(__LIT__("Lorem %ipsum"), __LIT__("em %ip"))')
        assert dbe.eval('CONTAINS("Карл у Клары украл кораллы", "Клары")')
        assert dbe.eval('CONTAINS(__LIT__("Карл у Клары украл кораллы"), __LIT__("Клары"))')
        # Non-string
        assert dbe.eval('CONTAINS(123456, "234")')
        assert not dbe.eval('CONTAINS(123456, "432")')
        assert dbe.eval('CONTAINS(TRUE, "ru")')
        assert dbe.eval('CONTAINS(#2019-03-04#, "019")')
        assert dbe.eval('CONTAINS(#2019-03-04T12:34:56#, "019")')

    @pytest.mark.parametrize("value_fl,pattern_fl,expected", CONTAINS_TESTS)
    def test_contains_extended(
        self,
        dbe: DbEvaluator,
        data_table: sa.Table,
        forced_literal_use: Any,
        value_fl: str,
        pattern_fl: str,
        expected: bool,
    ) -> None:
        if self.empty_str_is_null and pattern_fl == '""':
            # hopeless?
            return

        # const:
        statement = "CONTAINS(__LIT__({}), {})".format(value_fl, pattern_fl)
        assert dbe.eval(statement) is expected, (statement, expected)

        # var:
        statement = "CONTAINS(__LIT__({}), __LIT__({}))".format(value_fl, pattern_fl)
        assert dbe.eval(statement) is expected, (statement, expected)

    def test_icontains_simple(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('ICONTAINS("Lorem ipsum", "IPS")')
        assert not dbe.eval('ICONTAINS("Lorem ipsum", "ABC")')
        assert dbe.eval('ICONTAINS(__LIT__("Lorem ipsum"), __LIT__("IPS"))')
        assert not dbe.eval('ICONTAINS(__LIT__("Lorem ipsum"), __LIT__("ABC"))')
        assert dbe.eval('ICONTAINS("Lorem %ipsum", "EM %IP")')
        assert dbe.eval('ICONTAINS(__LIT__("Lorem %ipsum"), __LIT__("EM %IP"))')
        assert dbe.eval('ICONTAINS("Карл у Клары украл кораллы", "КЛАРЫ")')
        assert dbe.eval('ICONTAINS(__LIT__("Карл у Клары украл кораллы"), __LIT__("КЛАРЫ"))')
        # Quoting note: non-raw python string (backslash-escaped),
        # formulas string contents (backslash-escaped),
        # `like` pattern (backslash-escaped at a later point).
        assert dbe.eval('ICONTAINS("Lorem\\\\n %ipsum", "EM\\\\n %IP")')
        assert not dbe.eval('ICONTAINS("Lorem .ipsum", "EM %IP")')
        assert not dbe.eval('ICONTAINS("Lorem .ipsum", "EM \\%IP")')
        assert not dbe.eval('ICONTAINS("Lorem .ipsum", "EM \\\\%IP")')

        if self.supports_non_const_percent_escape:  # No proper escaping of a non-const value for MSSQL.
            assert dbe.eval('ICONTAINS(__LIT__("Lorem\\\\n\\\\% %ipsum"), __LIT__("EM\\\\n\\\\% %IP"))')
            assert not dbe.eval('ICONTAINS(__LIT__("Lorem .ipsum"), __LIT__("EM %IP"))')
            assert not dbe.eval('ICONTAINS(__LIT__("Lorem .ipsum"), __LIT__("EM \\%IP"))')
            assert not dbe.eval('ICONTAINS(__LIT__("Lorem .ipsum"), __LIT__("EM \\\\%IP"))')

        # Non-string
        assert dbe.eval('ICONTAINS(123456, "234")')
        assert not dbe.eval('ICONTAINS(123456, "432")')
        assert dbe.eval('ICONTAINS(123456.34, "234")')
        assert dbe.eval('ICONTAINS(TRUE, "ru")')
        assert dbe.eval('ICONTAINS(#2019-03-04#, "019")')
        assert dbe.eval('ICONTAINS(#2019-03-04T12:34:56#, "019")')

    def test_startswith_simple(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('STARTSWITH("Lorem ipsum", "Lore")')
        assert not dbe.eval('STARTSWITH("Lorem ipsum", "abc")')
        assert dbe.eval('STARTSWITH(__LIT__("Lorem ipsum"), __LIT__("Lore"))')
        assert not dbe.eval('STARTSWITH(__LIT__("Lorem ipsum"), __LIT__("abc"))')
        assert dbe.eval('STARTSWITH("Lorem% ipsum", "Lorem% ")')
        assert dbe.eval('STARTSWITH(__LIT__("Lorem% ipsum"), __LIT__("Lorem% "))')
        assert dbe.eval('STARTSWITH("Карл у Клары украл кораллы", "Карл")')
        assert dbe.eval('STARTSWITH(__LIT__("Карл у Клары украл кораллы"), __LIT__("Карл"))')
        # Non-string
        assert dbe.eval('STARTSWITH(123456, "123")')
        assert not dbe.eval('STARTSWITH(123456, "456")')
        assert dbe.eval('STARTSWITH(123456.34, "123")')
        assert dbe.eval('STARTSWITH(TRUE, "T")')
        assert dbe.eval('STARTSWITH(#2019-03-04#, "2019")')
        assert dbe.eval('STARTSWITH(#2019-03-04T12:34:56#, "2019")')

    def test_istartswith_simple(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('ISTARTSWITH("Lorem ipsum", "LORE")')
        assert not dbe.eval('ISTARTSWITH("Lorem ipsum", "ABC")')
        assert dbe.eval('ISTARTSWITH(__LIT__("Lorem ipsum"), __LIT__("LORE"))')
        assert not dbe.eval('ISTARTSWITH(__LIT__("Lorem ipsum"), __LIT__("ABC"))')
        assert dbe.eval('ISTARTSWITH("Lorem% ipsum", "LOREM% ")')
        assert dbe.eval('ISTARTSWITH(__LIT__("Lorem% ipsum"), __LIT__("LOREM% "))')
        assert dbe.eval('ISTARTSWITH("Карл у Клары украл кораллы", "КАРЛ")')
        assert dbe.eval('ISTARTSWITH(__LIT__("Карл у Клары украл кораллы"), __LIT__("КАРЛ"))')
        # Non-string
        assert dbe.eval('ISTARTSWITH(123456, "123")')
        assert not dbe.eval('ISTARTSWITH(123456, "456")')
        assert dbe.eval('ISTARTSWITH(123456.34, "123")')
        assert dbe.eval('ISTARTSWITH(TRUE, "T")')
        assert dbe.eval('ISTARTSWITH(#2019-03-04#, "2019")')
        assert dbe.eval('ISTARTSWITH(#2019-03-04T12:34:56#, "2019")')

    def test_endswith_simple(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('ENDSWITH("Lorem ipsum", "sum")')
        assert not dbe.eval('ENDSWITH("Lorem ipsum", "abc")')
        assert dbe.eval('ENDSWITH(__LIT__("Lorem ipsum"), __LIT__("sum"))')
        assert not dbe.eval('ENDSWITH(__LIT__("Lorem ipsum"), __LIT__("abc"))')
        assert dbe.eval('ENDSWITH("Lorem %ipsum", " %ipsum")')
        assert dbe.eval('ENDSWITH(__LIT__("Lorem %ipsum"), __LIT__(" %ipsum"))')
        assert dbe.eval('ENDSWITH("Карл у Клары украл кораллы", "аллы")')
        assert dbe.eval('ENDSWITH(__LIT__("Карл у Клары украл кораллы"), __LIT__("аллы"))')
        # Non-string
        assert dbe.eval('ENDSWITH(123456, "456")')
        assert not dbe.eval('ENDSWITH(123456, "123")')
        assert dbe.eval('ENDSWITH(123.456, "456")')
        assert dbe.eval('ENDSWITH(TRUE, "ue")')
        assert dbe.eval('ENDSWITH(#2019-03-04#, "04")')
        dt_end = self.datetime_str_ending
        assert dbe.eval(f'ENDSWITH(#2019-03-04T12:34:56#, "56{dt_end}")')

    def test_iendswith_simple(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('IENDSWITH("Lorem ipsum", "SUM")')
        assert not dbe.eval('IENDSWITH("Lorem ipsum", "ABC")')
        assert dbe.eval('IENDSWITH(__LIT__("Lorem ipsum"), __LIT__("SUM"))')
        assert not dbe.eval('IENDSWITH(__LIT__("Lorem ipsum"), __LIT__("ABC"))')
        assert dbe.eval('IENDSWITH("Lorem %ipsum", " %IPSUM")')
        assert dbe.eval('IENDSWITH(__LIT__("Lorem %ipsum"), __LIT__(" %IPSUM"))')
        assert dbe.eval('IENDSWITH("Карл у Клары украл кораллы", "АЛЛЫ")')
        assert dbe.eval('IENDSWITH(__LIT__("Карл у Клары украл кораллы"), __LIT__("АЛЛЫ"))')
        # Non-string
        assert dbe.eval('IENDSWITH(123456, "456")')
        assert not dbe.eval('IENDSWITH(123456, "123")')
        assert dbe.eval('IENDSWITH(123.456, "456")')
        assert dbe.eval('IENDSWITH(TRUE, "ue")')
        assert dbe.eval('IENDSWITH(#2019-03-04#, "04")')
        dt_end = self.datetime_str_ending
        assert dbe.eval(f'IENDSWITH(#2019-03-04T12:34:56#, "56{dt_end}")')

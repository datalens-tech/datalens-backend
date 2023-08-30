
from clickhouse_sqlalchemy.exceptions import DatabaseException
from clickhouse_sqlalchemy_tests.testcase import BaseTestCase


CASES = (
    (r"Code: 60, e.displayText() = DB::Exception: Table default.abc "
     r"doesn't exist. (version 19.16.2.2 (official build))"),
    (r"Code: 53. Type mismatch in VALUES section. Repeat query with "
     r"types_check=True for detailed info. Column x: argument out of range"),
    (r"std::exception. Code: 1001, type: ...::TErrorException, "
     r"e.what() = Error resolving path ..."),
)


class ExceptionParsingTestCase(BaseTestCase):

    def test_exception_parsing(self):
        for case in CASES:
            exc = DatabaseException(Exception(case))
            self.assertIsNotNone(exc.code)

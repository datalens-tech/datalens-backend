from contextlib import redirect_stdout
import io
from typing import (
    ClassVar,
    Type,
)

import attr

from dl_cli_tools.cli_base import CliToolBase


@attr.s(frozen=True)
class CliResult:
    stdout: str = attr.ib(kw_only=True)


class CliRunner:
    cli_cls: ClassVar[Type[CliToolBase]]

    def run_with_args(self, argv: list[str]) -> CliResult:
        with redirect_stdout(io.StringIO()) as out_stream:
            self.cli_cls.run(argv)

        assert isinstance(out_stream, io.StringIO)
        result = CliResult(stdout=out_stream.getvalue())
        return result

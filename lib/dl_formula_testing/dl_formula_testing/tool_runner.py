from __future__ import annotations

from contextlib import (
    contextmanager,
    redirect_stderr,
    redirect_stdout,
)
import io
import sys
from typing import (
    TYPE_CHECKING,
    Generator,
    Optional,
    Protocol,
    TextIO,
)


if TYPE_CHECKING:
    import argparse


class Tool(Protocol):
    @classmethod
    def run(cls, args: argparse.Namespace) -> None:
        raise NotImplementedError()


@contextmanager
def redirect_stdin(stream: Optional[TextIO] = None) -> Generator[None, None, None]:
    if stream is None:
        yield

    else:
        old_stdin = sys.stdin
        try:
            sys.stdin = stream
            yield
        finally:
            sys.stdin = old_stdin


class ToolRunner:
    def __init__(self, parser: argparse.ArgumentParser, tool_cls: type[Tool]):
        self.parser = parser
        self.tool_cls = tool_cls

    def run(self, args: list[str], stdin: Optional[str] = None):  # type: ignore  # 2024-01-29 # TODO: Function is missing a return type annotation  [no-untyped-def]
        stdin_str = stdin
        stdout = io.StringIO()
        stderr = io.StringIO()
        stdin_stream: Optional[TextIO] = None
        if stdin_str:
            stdin_stream = io.StringIO()
            assert stdin_stream is not None
            stdin_stream.write(stdin_str)
            stdin_stream.seek(0)
        with redirect_stdout(stdout), redirect_stderr(stderr), redirect_stdin(stdin_stream):
            self.tool_cls.run(self.parser.parse_args(args))  # noqa

        return stdout.getvalue(), stderr.getvalue()

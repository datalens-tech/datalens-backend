from contextlib import redirect_stdout
import io
from pathlib import Path
from typing import (
    ClassVar,
    Type,
)

import attr

from dl_repmanager.scripts.cli_base import CliToolBase
from dl_repmanager.scripts.package_meta_cli import DlPackageMetaTool
from dl_repmanager.scripts.repmanager_cli import DlRepManagerTool


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


@attr.s
class RepoCliRunner(CliRunner):
    cli_cls = DlRepManagerTool

    repo_path: Path = attr.ib()

    def run_with_args(self, argv: list[str]) -> CliResult:
        argv = ["--base-path", str(self.repo_path)] + argv
        return super().run_with_args(argv)


@attr.s
class PackageCliRunner(CliRunner):
    cli_cls = DlPackageMetaTool

    package_path: Path = attr.ib()

    def run_with_args(self, argv: list[str]) -> CliResult:
        argv = ["--package-path", str(self.package_path)] + argv
        return super().run_with_args(argv)

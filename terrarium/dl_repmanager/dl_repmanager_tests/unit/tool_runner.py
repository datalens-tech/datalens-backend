from pathlib import Path

import attr

from dl_cli_tools.testing.tool_runner import (
    CliResult,
    CliRunner,
)
from dl_repmanager.scripts.package_meta_cli import DlPackageMetaTool
from dl_repmanager.scripts.repmanager_cli import DlRepManagerTool


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

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Optional

import attr
from git.repo.base import Repo as GitRepo

from dl_cli_tools.cli_base import CliToolBase
from dl_cli_tools.logging import setup_basic_logging
from dl_gitmanager.discovery import discover_repo
from dl_gitmanager.git_manager import GitManager


@attr.s
class GitManagerTool(CliToolBase):
    git_manager: GitManager = attr.ib(kw_only=True)

    @classmethod
    def get_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="Git Repository Management CLI")
        parser.add_argument("--repo-path", type=Path, help="Main repo path", default=Path.cwd())

        # mix-in parsers
        base_parser = argparse.ArgumentParser(add_help=False)
        base_parser.add_argument("--base", help="Base commit specifier", default="HEAD~1")

        head_parser = argparse.ArgumentParser(add_help=False)
        head_parser.add_argument("--head", help="Head commit specifier", default="HEAD")

        base_head_parser = argparse.ArgumentParser(add_help=False, parents=[base_parser, head_parser])

        absolute_parser = argparse.ArgumentParser(add_help=False)
        absolute_parser.add_argument("--absolute", action="store_true", help="Use absolute paths")

        # commands
        subparsers = parser.add_subparsers(title="command", dest="command")

        subparsers.add_parser(
            "diff-paths", parents=[base_head_parser, absolute_parser], help="List file paths with changes"
        )

        return parser

    def diff_paths(self, base: str, head: Optional[str], absolute: bool) -> None:
        diff_name_list = self.git_manager.get_diff_paths(base=base, head=head, absolute=absolute)
        print("\n".join(diff_name_list))

    @classmethod
    def initialize(cls, git_manager: GitManager) -> GitManagerTool:
        tool = cls(git_manager=git_manager)
        return tool

    @classmethod
    def run_parsed_args(cls, args: argparse.Namespace) -> None:
        git_repo = discover_repo(repo_path=args.repo_path)
        git_manager = GitManager(git_repo=git_repo)
        tool = cls.initialize(git_manager=git_manager)

        match args.command:
            case "diff-paths":
                tool.diff_paths(base=args.base, head=args.head, absolute=args.absolute)
            case _:
                raise RuntimeError(f"Got unknown command: {args.command}")


def main() -> None:
    setup_basic_logging()
    GitManagerTool.run(sys.argv[1:])


if __name__ == "__main__":
    main()

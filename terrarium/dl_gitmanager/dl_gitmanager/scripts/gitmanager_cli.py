from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import (
    Optional,
    TextIO,
)

import attr

from dl_cli_tools.cli_base import CliToolBase
from dl_cli_tools.logging import setup_basic_logging
from dl_gitmanager.discovery import discover_repo
from dl_gitmanager.git_manager import GitManager


@attr.s
class GitManagerTool(CliToolBase):
    input_text_io: TextIO = attr.ib(kw_only=True)
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

        range_diff_paths_parser = subparsers.add_parser(
            "range-diff-paths",
            parents=[base_head_parser, absolute_parser],
            help="List file paths with changes given as commit range",
        )
        range_diff_paths_parser.add_argument(
            "--only-added-commits", action="store_true", help="Inspect only commits that are added in head"
        )

        return parser

    def range_diff_paths(self, base: str, head: Optional[str], absolute: bool, only_added_commits: bool) -> None:
        diff_name_list = self.git_manager.get_range_diff_paths(
            base=base,
            head=head,
            absolute=absolute,
            only_missing_commits=only_added_commits,
        )
        print("\n".join(diff_name_list))

    @classmethod
    def initialize(cls, git_manager: GitManager) -> GitManagerTool:
        tool = cls(input_text_io=sys.stdin, git_manager=git_manager)
        return tool

    @classmethod
    def run_parsed_args(cls, args: argparse.Namespace) -> None:
        git_repo = discover_repo(repo_path=args.repo_path)
        git_manager = GitManager(git_repo=git_repo)
        tool = cls.initialize(git_manager=git_manager)

        match args.command:
            case "range-diff-paths":
                tool.range_diff_paths(
                    base=args.base,
                    head=args.head,
                    absolute=args.absolute,
                    only_added_commits=args.only_added_commits,
                )
            case _:
                raise RuntimeError(f"Got unknown command: {args.command}")


def main() -> None:
    setup_basic_logging()
    GitManagerTool.run(sys.argv[1:])


if __name__ == "__main__":
    main()

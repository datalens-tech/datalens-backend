from __future__ import annotations

import argparse
from pathlib import Path
import sys
import time
from typing import (
    Optional,
    TextIO,
)

import attr
from colorama import (
    Fore,
    Style,
)
from colorama import init as colorama_init

from dl_cli_tools.cli_base import CliToolBase
from dl_cli_tools.logging import setup_basic_logging
from dl_gitmanager.cherry_farmer import (
    CherryFarmer,
    CommitPickerStateIO,
    CommitRuntimeStateItem,
    CommitState,
)
from dl_gitmanager.discovery import discover_repo
from dl_gitmanager.git_manager import GitManager


class StopIter(Exception):
    pass


@attr.s
class GitManagerTool(CliToolBase):
    input_text_io: TextIO = attr.ib(kw_only=True)
    cherry_farmer: CherryFarmer = attr.ib(kw_only=True)

    @classmethod
    def get_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="Git Cherry Farmer CLI")
        parser.add_argument("--repo-path", type=Path, help="Main repo path", default=Path.cwd())
        parser.add_argument(
            "--state-file",
            type=Path,
            help="Path to state file",
            default=Path.cwd() / "cherry_pick.json",
        )

        # mix-in parsers
        src_branch_parser = argparse.ArgumentParser(add_help=False)
        src_branch_parser.add_argument("--src-branch", help="Source branch", required=True)

        dst_branch_parser = argparse.ArgumentParser(add_help=False)
        dst_branch_parser.add_argument("--dst-branch", help="Destination branch", required=True)

        src_dst_branch_parser = argparse.ArgumentParser(add_help=False, parents=[src_branch_parser, dst_branch_parser])

        commit_parser = argparse.ArgumentParser(add_help=False)
        commit_parser.add_argument("--commit", help="Commit ID", required=True)

        message_parser = argparse.ArgumentParser(add_help=False)
        message_parser.add_argument("-m", "--message", help="Message", default="")

        state_parser = argparse.ArgumentParser(add_help=False)
        state_parser.add_argument(
            "--state",
            choices=["picked", "ignored", "new"],
            help="New state for the commit",
            required=True,
        )

        # commands
        subparsers = parser.add_subparsers(title="command", dest="command")

        picked_parser = argparse.ArgumentParser(add_help=False)
        picked_parser.add_argument("--picked", action="store_true", help="Show picked commits")

        ignored_parser = argparse.ArgumentParser(add_help=False)
        ignored_parser.add_argument("--ignored", action="store_true", help="Show ignored commits")

        new_parser = argparse.ArgumentParser(add_help=False)
        new_parser.add_argument("--new", action="store_true", help="Show new commits")

        all_parser = argparse.ArgumentParser(add_help=False)
        all_parser.add_argument("-a", "--all", action="store_true", help="Show all commits")

        commit_type_flag_parser = argparse.ArgumentParser(
            add_help=False,
            parents=[picked_parser, ignored_parser, new_parser, all_parser],
        )

        subparsers.add_parser(
            "show",
            parents=[src_dst_branch_parser, commit_type_flag_parser],
            help="List file paths with changes given as commit range",
        )

        subparsers.add_parser(
            "mark",
            parents=[commit_parser, state_parser, message_parser],
            help="Mark commit as picked/ignored/new",
        )

        iter_parser = subparsers.add_parser(
            "iter",
            parents=[src_dst_branch_parser, commit_type_flag_parser],
            help="Iterate over commits and pick them interactively",
        )
        iter_parser.add_argument("--one", action="store_true", help="Show only one commit and quit after handling it.")

        return parser

    def _get_states(self, picked: bool, ignored: bool, new: bool, all: bool) -> set[CommitState]:
        result: set[CommitState] = set()
        if picked or all:
            result.add(CommitState.picked)
        if ignored or all:
            result.add(CommitState.ignored)
        if new or all:
            result.add(CommitState.new)

        if not result:
            # no flags means "all"
            return self._get_states(new=True, picked=False, ignored=False, all=True)
            
        return result

    def _print_commit_state(self, commit_state_item: CommitRuntimeStateItem) -> None:
        iso_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(commit_state_item.saved_state.timestamp))
        text = f"""
{Fore.RED}{commit_state_item.saved_state.commit_id}{Style.RESET_ALL}:
    {iso_time}
    Commit Message: {commit_state_item.commit_message.strip()}
    Picker State:   {commit_state_item.saved_state.commit_state.name}
    Picker Message: {commit_state_item.saved_state.message}
"""
        print(text)

    def _print_pick_suggestion(self, suggestion_item: CommitRuntimeStateItem) -> None:
        iso_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(suggestion_item.saved_state.timestamp))
        text = f"""
{Fore.CYAN}Suggestion (found commit in dst):
    {suggestion_item.saved_state.commit_id}:
        {iso_time} - Commit Message: {suggestion_item.commit_message.strip()}
{Style.RESET_ALL}"""
        print(text)

    def show(self, src_branch: str, dst_branch: str, picked: bool, ignored: bool, new: bool, all: bool) -> None:
        states = self._get_states(picked=picked, ignored=ignored, new=new, all=all)
        for commit_state_item in self.cherry_farmer.iter_diff_commits(
            src_branch=src_branch,
            dst_branch=dst_branch,
            states=states,
            reverse=True,
        ):
            self._print_commit_state(commit_state_item)

    def mark(self, commit_id: str, state: str, message: str) -> None:
        state_val = CommitState[state]
        commit_state_item = self.cherry_farmer.get_commit_state_item(commit_id=commit_id)
        self.cherry_farmer.mark(
            commit_id=commit_id,
            state=state_val,
            message=message,
            timestamp=commit_state_item.saved_state.timestamp,
        )

    def _prompt_mark_commit(self, commit_state_item: CommitRuntimeStateItem) -> tuple[CommitState, Optional[str]]:
        while True:
            raw_state = input(f'{Fore.GREEN}Mark as [picked|ignored|new] or "exit"> {Style.RESET_ALL}')
            if raw_state == "exit":
                raise StopIter

            try:
                state = CommitState[raw_state]
            except KeyError:
                print(f"Invalid state: {raw_state}")
            else:
                break

        if state == CommitState.new:
            message = ""
        else:
            message = input(f"{Fore.GREEN}Enter pick message > {Style.RESET_ALL}")

        return state, message

    def iter_(
        self,
        src_branch: str,
        dst_branch: str,
        picked: bool,
        ignored: bool,
        new: bool,
        all: bool,
        one: bool,
    ) -> None:
        states = self._get_states(picked=picked, ignored=ignored, new=new, all=all)
        for commit_state_item in self.cherry_farmer.iter_diff_commits(
            src_branch=src_branch,
            dst_branch=dst_branch,
            states=states,
        ):
            self._print_commit_state(commit_state_item)
            pick_suggestion_item = self.cherry_farmer.search_pick_suggestion(
                src_branch=src_branch,
                dst_branch=dst_branch,
                commit_id=commit_state_item.saved_state.commit_id,
            )
            if pick_suggestion_item is not None:
                self._print_pick_suggestion(pick_suggestion_item)
            try:
                state, message = self._prompt_mark_commit(commit_state_item=commit_state_item)
            except StopIter:
                return

            self.cherry_farmer.mark(
                commit_id=commit_state_item.saved_state.commit_id,
                state=state,
                message=message,
                timestamp=commit_state_item.saved_state.timestamp,
            )
            if one:
                # quit after just one handled commit
                break

    @classmethod
    def initialize(cls, cherry_farmer: CherryFarmer) -> GitManagerTool:
        tool = cls(input_text_io=sys.stdin, cherry_farmer=cherry_farmer)
        return tool

    @classmethod
    def run_parsed_args(cls, args: argparse.Namespace) -> None:
        colorama_init()

        git_repo = discover_repo(repo_path=args.repo_path)
        state_io = CommitPickerStateIO()
        git_manager = GitManager(git_repo=git_repo)

        with state_io.load_save_state(path=args.state_file) as picker_state:
            cherry_farmer = CherryFarmer(git_manager=git_manager, state=picker_state)
            tool = cls.initialize(cherry_farmer=cherry_farmer)
            match args.command:
                case "show":
                    tool.show(
                        src_branch=args.src_branch,
                        dst_branch=args.dst_branch,
                        picked=args.picked,
                        ignored=args.ignored,
                        new=args.new,
                        all=args.all,
                    )
                case "mark":
                    tool.mark(
                        commit_id=args.commit,
                        state=args.state,
                        message=args.message,
                    )
                case "iter":
                    tool.iter_(
                        src_branch=args.src_branch,
                        dst_branch=args.dst_branch,
                        picked=args.picked,
                        ignored=args.ignored,
                        new=args.new,
                        all=args.all,
                        one=args.one,
                    )
                case _:
                    raise RuntimeError(f"Got unknown command: {args.command}")


def main() -> None:
    setup_basic_logging()
    GitManagerTool.run(sys.argv[1:])


if __name__ == "__main__":
    main()

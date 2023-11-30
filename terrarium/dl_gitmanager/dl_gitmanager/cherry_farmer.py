from __future__ import annotations

from contextlib import contextmanager
import datetime
from enum import (
    Enum,
    unique,
)
import json
from pathlib import Path
from typing import (
    AbstractSet,
    Any,
    Generator,
    Iterable,
    Iterator,
    Optional,
)

import attr
from git.objects.commit import Commit

from dl_gitmanager.git_manager import GitManager


@unique
class CommitState(Enum):
    new = "new"
    picked = "picked"
    ignored = "ignored"


@attr.s(frozen=True)
class CommitSavedStateItem:
    commit_id: str = attr.ib(kw_only=True)
    commit_state: CommitState = attr.ib(kw_only=True)
    message: Optional[str] = attr.ib(kw_only=True)
    timestamp: int = attr.ib(kw_only=True)


@attr.s(frozen=True)
class CommitRuntimeStateItem:
    saved_state: CommitSavedStateItem = attr.ib(kw_only=True)
    commit_message: str = attr.ib(kw_only=True)


@attr.s
class CommitPickerState:
    _commit_state_items: dict[str, CommitSavedStateItem] = attr.ib(kw_only=True)

    def mark(
        self,
        commit_id: str,
        state: CommitState,
        message: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> None:
        if state == CommitState.new:
            if commit_id in self._commit_state_items:
                del self._commit_state_items[commit_id]

        else:
            assert message is not None
            assert timestamp is not None
            commit_saved_state_item = CommitSavedStateItem(
                commit_id=commit_id,
                commit_state=state,
                message=message,
                timestamp=timestamp,
            )
            self._commit_state_items[commit_id] = commit_saved_state_item

    def __iter__(self) -> Iterator[CommitSavedStateItem]:
        return iter(sorted(self._commit_state_items.values(), key=lambda item: item.timestamp))

    def __contains__(self, item: Any) -> bool:
        if not isinstance(item, str):
            raise TypeError(type(item))
        return item in self._commit_state_items

    def __getitem__(self, item: Any) -> CommitSavedStateItem:
        if not isinstance(item, str):
            raise TypeError(type(item))
        return self._commit_state_items[item]

    def clone(self) -> CommitPickerState:
        return attr.evolve(self, commit_state_items=self._commit_state_items.copy())


class CommitPickerStateIO:
    def load_from_file(self, path: Path) -> CommitPickerState:
        with open(path, "r") as state_file:
            raw_list = json.load(state_file)

        commit_state_items: dict[str, CommitSavedStateItem] = {}
        for raw_item in raw_list:
            commit_saved_state_item = CommitSavedStateItem(
                commit_id=raw_item["commit_id"],
                timestamp=raw_item["timestamp"],
                commit_state=CommitState(raw_item["commit_state"]),
                message=raw_item["message"],
            )
            commit_state_items[commit_saved_state_item.commit_id] = commit_saved_state_item

        return CommitPickerState(commit_state_items=commit_state_items)

    def save_to_file(self, path: Path, picker_state: CommitPickerState) -> None:
        raw_list: list[dict] = []
        for commit_saved_state_item in picker_state:
            raw_item = dict(
                commit_id=commit_saved_state_item.commit_id,
                timestamp=commit_saved_state_item.timestamp,
                commit_state=commit_saved_state_item.commit_state.name,
                message=commit_saved_state_item.message,
            )
            raw_list.append(raw_item)

        with open(path, "w") as state_file:
            json.dump(raw_list, state_file, sort_keys=True, indent=4)

    @contextmanager
    def load_save_state(self, path: Path) -> Generator[CommitPickerState, None, None]:
        picker_state = self.load_from_file(path=path)
        original_state = picker_state.clone()
        yield picker_state
        if picker_state != original_state:
            self.save_to_file(path=path, picker_state=picker_state)


@attr.s
class CherryFarmer:
    _state: CommitPickerState = attr.ib(kw_only=True)
    _git_manager: GitManager = attr.ib(kw_only=True)

    def _make_commit_runtime_state_item(self, commit_obj: Commit) -> CommitRuntimeStateItem:
        commit_id = commit_obj.hexsha
        commit_saved_state_item: CommitSavedStateItem
        if commit_id in self._state:
            commit_saved_state_item = self._state[commit_id]
        else:
            commit_saved_state_item = CommitSavedStateItem(
                commit_id=commit_id,
                commit_state=CommitState.new,
                message="",
                timestamp=commit_obj.committed_date,
            )

        return CommitRuntimeStateItem(
            saved_state=commit_saved_state_item,
            commit_message=commit_obj.message,
        )

    def iter_diff_commits(
        self,
        src_branch: str,
        dst_branch: str,
        states: AbstractSet[CommitState],
        reverse: bool = False,
    ) -> Generator[CommitRuntimeStateItem, None, None]:
        commits: Iterable[Commit]
        commits = self._git_manager.iter_commits(base=dst_branch, head=src_branch, only_missing_commits=True)
        if not reverse:  # Commits are in reverse order
            commits = reversed(list(commits))

        for commit_obj in commits:
            commit_runtime_state_item = self._make_commit_runtime_state_item(commit_obj=commit_obj)
            if commit_runtime_state_item.saved_state.commit_state in states:
                yield commit_runtime_state_item

    def get_commit_state_item(self, commit_id: str) -> CommitRuntimeStateItem:
        commit_obj = self._git_manager.get_commit_obj(commit_specifier=commit_id)
        return self._make_commit_runtime_state_item(commit_obj=commit_obj)

    def mark(
        self,
        commit_id: str,
        state: CommitState,
        message: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> None:
        if message == "":
            message = f"Set to state {state.name!r} at {datetime.datetime.utcnow().isoformat()}"
        self._state.mark(commit_id=commit_id, state=state, message=message, timestamp=timestamp)

    def search_pick_suggestion(
        self,
        commit_id: str,
        src_branch: str,
        dst_branch: str,
    ) -> Optional[CommitRuntimeStateItem]:
        """Search for a commit that might be a cherry pick of another commit"""
        # Get commits with reversed branch roles
        commits = self._git_manager.iter_commits(base=src_branch, head=dst_branch, only_missing_commits=True)
        for commit_obj in commits:
            if commit_id in commit_obj.message:
                return self._make_commit_runtime_state_item(commit_obj=commit_obj)

        return None

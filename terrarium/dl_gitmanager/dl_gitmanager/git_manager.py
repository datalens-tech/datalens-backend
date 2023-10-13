from __future__ import annotations

from pathlib import Path
from typing import Generator

import attr
from git.diff import Diff
from git.repo.base import Repo as GitRepo


@attr.s
class GitManager:
    git_repo: GitRepo = attr.ib(kw_only=True)

    def get_root_path(self) -> Path:
        return Path(self.git_repo.working_tree_dir)

    def get_head_commit(self) -> str:
        return self.git_repo.head.commit.hexsha

    def get_commit(self, commit_specifier: str) -> str:
        return self.git_repo.commit(commit_specifier).hexsha

    def _iter_diffs(self, base: str, head: str, absolute: bool = False) -> Generator[tuple[Path, Diff], None, None]:
        base_commit = self.git_repo.commit(base)
        head_commit = self.git_repo.commit(head)

        base_path: Path
        if absolute:
            base_path = self.get_root_path()
        else:
            base_path = Path(".")

        # Iter own diffs
        diff_index = head_commit.diff(base_commit)
        for diff_item in diff_index:
            yield base_path, diff_item

        # Iter submodules and get their internal diffs
        for submodule in self.git_repo.submodules:
            submodule_name = submodule.name
            base_tree_item = base_commit.tree[submodule_name]
            head_tree_item = head_commit.tree[submodule_name]
            submodule_base = base_tree_item.hexsha
            submodule_head = head_tree_item.hexsha
            submodule_manager = self.get_submodule_manager(submodule_name=submodule_name)

            submodule_base_path: Path
            if absolute:
                submodule_base_path = submodule_manager.get_root_path()
            else:
                submodule_base_path = Path(submodule.path)

            # Iterate. Override the repo paths here with the submodule path
            for _, diff_item in submodule_manager._iter_diffs(base=submodule_base, head=submodule_head):
                yield submodule_base_path, diff_item

    def get_diff_paths(self, base: str, head: str, absolute: bool = False) -> list[str]:
        result: set[str] = set()
        for base_path, diff_item in self._iter_diffs(base=base, head=head, absolute=absolute):
            if diff_item.a_path:
                result.add(str(base_path / diff_item.a_path))
            if diff_item.b_path:
                result.add(str(base_path / diff_item.b_path))

        return sorted(result)

    def get_submodule_manager(self, submodule_name: str) -> GitManager:
        submodule_dict = {sm.name: sm for sm in self.git_repo.submodules}
        submodule = submodule_dict[submodule_name]
        return type(self)(git_repo=submodule.module())

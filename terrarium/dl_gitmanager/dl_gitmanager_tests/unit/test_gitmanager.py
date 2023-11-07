import pytest

from dl_gitmanager.git_manager import GitManager
from dl_gitmanager_tests.unit.git_tools import (
    AddGitFileAction,
    GitActionProcessor,
    GitSubmoduleAction,
    MultiGitAction,
    SubmoduleSpec,
    UpdateGitSubmoduleAction,
)


def _norm_paths(action: MultiGitAction) -> frozenset[str]:
    return frozenset(str(path) for path in action.get_paths())


def test_check_linear_range_diffs(git_action_proc: GitActionProcessor, git_manager: GitManager) -> None:
    action = MultiGitAction(
        actions=(
            AddGitFileAction(path=git_action_proc.generate_path()),
            AddGitFileAction(path=git_action_proc.generate_path()),
        )
    )
    git_action_proc.add_commit(action=action, message="My new commit")

    expected_diff_paths = _norm_paths(action)
    actual_diff_paths = frozenset(git_manager.get_range_diff_paths(base="HEAD~1", head="HEAD", absolute=True))
    assert actual_diff_paths == expected_diff_paths


def test_check_range_diffs_with_branches(git_action_proc: GitActionProcessor, git_manager: GitManager) -> None:
    git_action_proc.checkout_new_branch("my_branch")
    action_my_branch_1 = MultiGitAction(
        actions=(
            AddGitFileAction(path=git_action_proc.generate_path()),
            AddGitFileAction(path=git_action_proc.generate_path()),
        )
    )
    git_action_proc.add_commit(action=action_my_branch_1, message="My first commit")
    action_my_branch_2 = MultiGitAction(actions=(AddGitFileAction(path=git_action_proc.generate_path()),))
    git_action_proc.add_commit(action=action_my_branch_2, message="My second commit")

    git_action_proc.checkout_existing_branch("main")
    action_main_1 = MultiGitAction(
        actions=(
            AddGitFileAction(path=git_action_proc.generate_path()),
            AddGitFileAction(path=git_action_proc.generate_path()),
        )
    )
    git_action_proc.add_commit(action=action_main_1, message="My main commit")

    # With only_missing_commits=True
    expected_diff_paths = _norm_paths(action_my_branch_1) | _norm_paths(action_my_branch_2)
    actual_diff_paths = frozenset(
        git_manager.get_range_diff_paths(
            base="main",
            head="my_branch",
            absolute=True,
            only_missing_commits=True,
        )
    )
    assert actual_diff_paths == expected_diff_paths

    # With only_missing_commits=False
    expected_diff_paths = _norm_paths(action_my_branch_1) | _norm_paths(action_my_branch_2) | _norm_paths(action_main_1)
    actual_diff_paths = frozenset(
        git_manager.get_range_diff_paths(
            base="main",
            head="my_branch",
            absolute=True,
            only_missing_commits=False,
        )
    )
    assert actual_diff_paths == expected_diff_paths


@pytest.mark.skip("Some problems with managing submodule")
def test_check_range_diffs_with_branches_and_submodules(
    git_action_proc: GitActionProcessor,
    git_manager: GitManager,
) -> None:
    submodule = SubmoduleSpec(
        name="my_sub", path=git_action_proc.generate_path(), url="https://github.com/aio-libs/yarl.git"
    )
    git_action_proc.add_commit(message="Add submodule", action=GitSubmoduleAction(submodule=submodule))
    sm_action_proc = git_action_proc.get_submodule_proc(submodule=submodule)
    sm_action_proc.checkout_new_branch("main")

    sm_action_proc.checkout_new_branch("my_sm_branch")
    action_sm_my_branch_1 = MultiGitAction(actions=(AddGitFileAction(path=sm_action_proc.generate_path()),))
    sm_commit_1 = sm_action_proc.add_commit(action=action_sm_my_branch_1, message="My first SM commit")

    git_action_proc.checkout_new_branch("my_branch")
    action_my_branch_1 = MultiGitAction(
        actions=(
            AddGitFileAction(path=git_action_proc.generate_path()),
            AddGitFileAction(path=git_action_proc.generate_path()),
            UpdateGitSubmoduleAction(submodule=submodule, new_commit=sm_commit_1),
        )
    )
    git_action_proc.add_commit(action=action_my_branch_1, message="My first commit")

    sm_action_proc.checkout_existing_branch("main")
    action_sm_main_1 = MultiGitAction(actions=(AddGitFileAction(path=sm_action_proc.generate_path()),))
    sm_commit_2 = sm_action_proc.add_commit(action=action_sm_main_1, message="My main SM commit")

    sm_action_proc.checkout_commit(sm_commit_1)  # To revert changes in main repo
    git_action_proc.checkout_existing_branch("main")
    action_main_1 = MultiGitAction(
        actions=(
            AddGitFileAction(path=git_action_proc.generate_path()),
            AddGitFileAction(path=git_action_proc.generate_path()),
            UpdateGitSubmoduleAction(submodule=submodule, new_commit=sm_commit_2),
        )
    )
    git_action_proc.add_commit(action=action_main_1, message="My main commit")

    # With only_missing_commits=True
    # Note that for SM all commits are included - added and removed
    expected_diff_paths = (
        _norm_paths(action_my_branch_1) | _norm_paths(action_sm_my_branch_1) | _norm_paths(action_sm_main_1)
    )
    actual_diff_paths = frozenset(
        git_manager.get_range_diff_paths(
            base="main",
            head="my_branch",
            absolute=True,
            only_missing_commits=True,
        )
    )
    assert actual_diff_paths == expected_diff_paths

    # With only_missing_commits=False
    expected_diff_paths = (
        _norm_paths(action_my_branch_1)
        | _norm_paths(action_main_1)
        | _norm_paths(action_sm_my_branch_1)
        | _norm_paths(action_sm_main_1)
    )
    actual_diff_paths = frozenset(
        git_manager.get_range_diff_paths(
            base="main",
            head="my_branch",
            absolute=True,
            only_missing_commits=False,
        )
    )
    assert actual_diff_paths == expected_diff_paths

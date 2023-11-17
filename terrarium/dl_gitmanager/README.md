# dl_gitmanager

Tool for advanced/.custom git operations

## Installation

```bash
pip install -Ue <path-to-package>
```

## Common options

### --help

Show the main help message

```bash
dl-git --help
dl-git --h
```

The `--help` (`-h`) option can also be used for any command:
```bash
dl-git <command> --help
```

### --repo-path

Optional. Specify custom repository path

```
dl-git --repo-path <command> <args>
```

By default CWD is used.
The tool first looks for the repository at the given path, and then, if not found,
it starts a recursive search upwards, toward the path root. If no repository is found, an error is raised.
If the path is inside a submodule, then the submodule is considered to be the root repo.

## Commands

### range-diff-paths

List files that have changed between two given revisions including the changes in all submodules

```bash
dl-git range-diff-paths --base <base> --head <head>
dl-git range-diff-paths --base <base> --head <head> --absolute
dl-git range-diff-paths --base <base> --head <head> --only-added-commits
```

Here `base` and `head` can be a commit ID, branch name, `HEAD~3` or any similar notation
that is usually accepted by git.
These arguments are optional. By default `head` is `HEAD` and `base` is `HEAD~1`.
Thgis means you can use the following command to see the diff of the last commit in the current branch:
```bash
dl-git range-diff-paths
```

The `--absolute` option makes the tool print absolute paths.
By default they are printed relative to the repository root.

The `--only-added-commits` option makes the tool inspect only commits
that have been added in the head version.

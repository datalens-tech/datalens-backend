# dl_gitmanager

Tool for advanced/.custom git operations

## Installation

```bash
pip install -Ue <path-to-package>
```

## Common options

### --help

Show the main help message

```
dl-git --help
dl-git --h
```

The `--help` (`-h`) option can also be used for any command:
```
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

```
dl-git range-diff-paths --base <base> --head <head>
dl-git range-diff-paths --base <base> --head <head> --absolute
dl-git range-diff-paths --base <base> --head <head> --only-added-commits
dl-git range-diff-paths --base <base> --head <head> --without-deleted-files
```

Here `base` and `head` can be a commit ID, branch name, `HEAD~3` or any similar notation
that is usually accepted by git.
These arguments are optional. By default `head` is `HEAD` and `base` is `HEAD~1`.
This means you can use the following command to see the diff of the last commit in the current branch:
```
dl-git diff-paths
```

The `--absolute` option makes the tool print absolute paths.
By default they are printed relative to the repository root.

The `--only-added-commits` option makes the tool inspect only commits
that have been added in the head version.

The `--without-deleted-files` option makes the tool omit deleted (non-existent) files in the resulting list.

### list-diff-paths

List files that have changed in commits passed on as input

```
echo <commit-id> | dl-git list-diff-paths
echo <commit-id> | dl-git list-diff-paths --absolute
echo <commit-id> | dl-git list-diff-paths --without-deleted-files
```

Options `--absolute` and `--without-deleted-files` have the same meaning as in `range-diff-paths`.

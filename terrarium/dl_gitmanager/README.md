# dl_gitmanager

Tool for advanced/.custom git operations

## Installation

```bash
pip install -Ue <path-to-package>
```


# dl-git

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
dl-git --repo-path <path> <command> <args>
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


# dl-cherry-farmer

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
dl-cherry-farmer --repo-path <command> <args>
```

By default CWD is used.
The tool first looks for the repository at the given path, and then, if not found,
it starts a recursive search upwards, toward the path root. If no repository is found, an error is raised.
If the path is inside a submodule, then the submodule is considered to be the root repo.

### --state-file

Optional. Specify path to cherry-farmer's state file

```
dl-cherry-farmer --state-file <path>  <command> <args>
```

By default `./cherry_farmer.json` is used.

## Commands

### show

```bash
dl-cherry-farmer show --src-branch <branch> --dst-branch <branch>
dl-cherry-farmer show --src-branch <branch> --dst-branch <branch> --new
dl-cherry-farmer show --src-branch <branch> --dst-branch <branch> --ignored
dl-cherry-farmer show --src-branch <branch> --dst-branch <branch> --picked
dl-cherry-farmer show --src-branch <branch> --dst-branch <branch> --all
```

Here `--src-branch` and `--dst-branch` are the names of the branches that you want to compare

Several options control which commits to show:
- `--new`: new (not picked and not ignored commits) will be shown.
- `--picked`: picked commits will be shown.
- `--ignored`: ignored commits will be shown.
- `-a` or `--all`: all commits will be shown.

If none of these is specified, then `--new` is assumed.


### iter

Iterate over all commits in diff and pick or ignore them interactively

```bash
dl-cherry-farmer show --src-branch <branch> --dst-branch <branch>
dl-cherry-farmer show --src-branch <branch> --dst-branch <branch> --new
dl-cherry-farmer show --src-branch <branch> --dst-branch <branch> --one
```

Accepts the same options as does the `show` command, but has some additional ones:
- `--one`: instead of iterating over all commits show just one and quit after it is handled.

The tool will ask for a new state and message for each commit.
If the message is not specified, a default one will be generated.

To skip an un-picked commit (and not mark it as `ignored` or `picked`) use the state `new`.


### mark

Non-interactive command for marking commits.

```bash
dl-cherry-farmer mark --commit <commit> --state <state>
dl-cherry-farmer mark --commit <commit> --state <state> -m <message>
dl-cherry-farmer mark --commit <commit> --state <state> --message <message>
```

Here `commit` should be a valid commit ID, and `state` is the state to be set for this commit.
If `--state new` is specified, then the info about this commit is simply deleted from the saved state.
Use `-m` or `--message` to specify a pick message (will be saved in the state file).
If the message is not specified, a default one will be generated.

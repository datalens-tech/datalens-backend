## Management tasks

### Environment (`env:`)

```
task env:docker-reinstall
```
Re-install remote Python interpreter with all the repository packages

```
task env:docker-remote
```
Open shell with the remote Python interpreter

```
task env:devend
task env:devend-d
```
Create development/testing environment (run it from a package dir)


### Generation (`gen:`)

```
task gen:antlr
```
(Re-)generate ANTLR code files for formula

```
task gen:i18n-po
```
Sync/generate `.po` files for package (run it from a package dir)

```
task gen:i18n-binaries
```
Generate binary `.mo` files from `.po` files for package (run it from a package dir)

```
task gen:formula-ref -- <config_version>
```
Generate formula reference for all supported locales for the given `config_version`.
Resulting doc files are placed in `artifacts/generated_docs`


### Repository management (`rep:`)

```
task rep:poetry-lock
```
(Re-)generate main `poetry.lock` file


```
task rep:mypy
task rep:mypy -- <relative/path>
```
Run mypy for the whole repository
(run it either without args from the dir that you want to check or pass the path after `--`)


### Publication (`pub:`)

Tools to sync code to pre-public repo. Will be obsolete after final publication.

```
task pub:prepublic-file-sync
```
Commit and push current `mainrepo/` dir to pre-public repo `trunk` branch.
`PUB_SYNC_TARGET_REPO_DIR_PREPUBLIC` env var required: local path with cloned pre-public repo

All local changes in pre-public repo **will be reset** and **trunk** will be checked out and pulled.
Current `mainrepo/` (**including local changes**) will be dumped (only files added to GIT).


### [Mainrepo] Code quality (`cq:`)

Experimental task to check and fix source files. 
- task cq:fix_changed : applies all auto-fixes
- task cq:check_changed : check for any non-conformity in code style/format/lint
- task cq:fix_dir -- {single dir} : applies all auto-fixes to the given dir absolute path
- task cq:check_dir -- {single dir} : check for any non-conformity in code style/format/lint in the given dir abs path

### [Mainrepo] Main env (`main_env`)

Command to create virtual env for the mainrepo tools. It requires presence of .env in the mainrepo/tools.
```
task main_env:ensure_venv
```

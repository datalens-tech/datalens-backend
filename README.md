## Management tasks

For `mainrepo` commands See the [mainrepo README](mainrepo/README.md)

### Environment (`env-ya:`)

- `task env-ya:docker-reinstall`:
  Re-install remote Python interpreter with all the repository packages
- `task env-ya:docker-remote`:
  Open shell with the remote Python interpreter
- `task env-ya:mainrepo-tasks`:
  (Re)deploy taskfile override for mainrepo so that the same commands are supported
  in mainrepoo as the ones in the internal repo root 

See [mainrepo README](mainrepo/README.md) for other environment tasks.


### Generation (`gen-ya:`)

- `task gen-ya:i18n-po`:
  Sync/generate `.po` files for package (run it from a package dir)
- `task gen-ya:i18n-binaries`:
  Generate binary `.mo` files from `.po` files for package (run it from a package dir)
- `task gen-ya:formula-ref -- <config_version>`:
  Generate formula reference for all supported locales for the given `config_version`.
  Resulting doc files are placed in `artifacts/generated_docs`

See [mainrepo README](mainrepo/README.md) for other generation tasks.


### Repository management (`rep-ya:`)

- `task rep-ya:poetry-lock`:
  (Re-)generate main `poetry.lock` file
- `task rep-ya:mypy`:
  Run mypy for the current directory
- `task rep-ya:mypy -- <relative/path>`:
  Run mypy for the path relative to the current directory


### Publication (`pub-ya:`)

Tools to sync code to pre-public repo. Will be obsolete after final publication.

```
task pub-ya:prepublic-file-sync
```
Commit and push current `mainrepo/` dir to pre-public repo `trunk` branch.
`PUB_SYNC_TARGET_REPO_DIR_PREPUBLIC` env var required: local path with cloned pre-public repo

All local changes in pre-public repo **will be reset** and **trunk** will be checked out and pulled.
Current `mainrepo/` (**including local changes**) will be dumped (only files added to GIT).


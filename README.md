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
Generate formula reference for all locales for the given `config_version`.
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

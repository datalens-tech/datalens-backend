## Management tasks

These commands require the ``taskfile`` tool. See [this page](https://taskfile.dev/installation/)
for installation options.

Running tasks:
```
task <task_name>
```


### Environment (`env:`)

Working with the testing/development environment.

- `task env:devenv`:
  Create development/testing environment (run it from a package dir)
- `task env:devenv-d`:
  Create development/testing environment in detached mode (run it from a package dir)
- `task env:ensure_venv`: Command to create virtual env for the mainrepo tools.
  It requires presence of .env in the mainrepo/tools.


### Generation (`gen:`)

Generating files to be used from the code.

- `task gen:antlr`:
  (Re-)generate ANTLR code files for formula
- `task gen:i18n-po`:
  Sync/generate `.po` files for package (run it from a package dir)
- `task gen:i18n-binaries`:
  Generate binary `.mo` files from `.po` files for package (run it from a package dir)

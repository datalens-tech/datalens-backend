# dl_repmanager

Package containing tools for package/repo management.


## Installation

```bash
pip install -Ue <path-to-package>
```


# `dl-package`

Tool for working with metadata of individual packages.


## Commands


### list-i18n-domains

List i18n domains of a package

```bash
dl-package --package-path <path> list-i18n-domains

# example:
dl-package --package-path lib/dl_connector_clickhouse list-i18n-domains
```
Will print something like:
```
dl_connector_clickhouse=dl_connector_clickhouse/api;dl_connector_clickhouse/core
dl_formula_ref_dl_connector_clickhouse=dl_connector_clickhouse/formula_ref
```


### set-meta-array

Set array property in package's `pyproject.toml`
```bash
dl-package --package-path <path> set-meta-array --toml-section <section_name> --toml-key <property_key> --toml-value <semicolon;separated;values>

# example:
dl-package --package-path lib/dl_core set-meta-array --toml-section tool.poetry --toml-key authors --toml-value "Alicia <alicia@site.com>";James <james@site.com>"
```


### set-meta-text

Set text property in package's `pyproject.toml`
```bash
dl-package --package-path <path> set-meta-text --toml-section <section_name> --toml-key <property_key> --toml-value <property value>

# example:
dl-package --package-path lib/dl_core set-meta-text --toml-section tool.poetry --toml-key license --toml-value "Apache 2.0"
```


# `dl-repo`

Tool for managing and inspecting packages as part of the repository and the repository as a whole.
This includes creation, renaming, moving of packages, etc. with updating all the necessary dependencies and registries.


## Common options


### --help

Show the main help message or help for a specific command

```
dl-repo --help
dl-repo --h
dl-repo <command> -h
```


### --config

Optional. Path to the repository configuration file (`dl-repo.yml`).
By default the tool will discover the config automatically going up from the CWD.

See [dl-repo.yml](../../dl-repo.yml).

```bash
dl-repo --config /my/custom/repo-config.yml <command> ...
```


### --fs-editor

Controls how FS operations are performed

Possible values:
- `default`
- `git`
- `virtual`

The default is set in the repo config ([dl-repo.yml](../../dl-repo.yml)). If not, the default is `default`.

```bash
dl-repo --fs-editor git <command> ...
```


## Commands


### ch-package-type

Change the type of (essentially, move) a package.

```bash
dl-repo ch-package-type --package-name <package_name> --package-type <new_package_type>

# example:
dl-repo ch-package-type --package-name dl_core --package-type app
```

The `--package-name` argument is the name of the package to move. This argument is required.

The `--package-type` argument specifies the name of the package's new type, which maps to a ceertain folder in the repo.
See the repo config ([dl-repo.yml](../../dl-repo.yml)). This argument is required.


### compare-resulting-deps

*TODO*


### copy

Create a new package as a copy of an existing one.

```bash
dl-repo copy --package-name <new_package_name> --from-package-name <package_to_copy>

# example:
dl-repo copy --package-name dl_super_package --from-package-name dl_core
```

The `--package-name` argument is the name of the new package's folder and module name. This argument is required.

The `--from-package-name` argument of the package to copy (to use as the boilerplate). This argument is required.


### ensure-mypy-common

*TODO*


### import-list

*TODO*


### init

Create a new package

```bash
dl-repo init --package-type <package_type> --package-name <new_package_name>

# example:
dl-repo init --package-type lib --package-name dl_super_package
```

The `--package-type` argument specifies where the package will be created. Package types are configured
in the repo config ([dl-repo.yml](../../dl-repo.yml)). This argument is required.

The `--package-name` argument is the name of the new package's folder and module name. This argument is required.


### package-list

*TODO*


### recurse-packages

*TODO*


### rename

Rename a package.

```bash
dl-repo rename --package-name <package_name> --new-package-name <new_package_name>

# example:
dl-repo rename --package-name dl_core --new-package-name dl_ultra_core
```

The `--package-name` argument is the original name of the package to rename. This argument is required.

The `--new-package-name` argument Is the new name of the package.


### rename-module

*TODO*


### req-check

Check whether the requirements of a package are consistent with imports in the package's code.

```bash
dl-repo req-check --package-name <package_name>
dl-repo req-check --package-name <package_name> --test
```

The `--package-name` argument is the original name of the package to check.

The `--tests` flag tells the tool to check test requirements instead of the main ones.


### req-list

List requirements of a package

```bash
dl-repo req-list --package-name <package_name>
```


### resolve

*TODO*


### rm

Remove a package.

```bash
dl-repo rm --package-name <package_name>

# example:
dl-repo rm --package-name dl_core
```

The `--package-name` argument is the name of the package to remove. This argument is required.


### search-imports

*TODO*

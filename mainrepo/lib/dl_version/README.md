# dl_version

Package contains only united version of all packages in repository.
Version is bumped automatically on release (see `.bumpversion.cfg`)
Every package in private repo **MUST** use `dl_version.__version__` to get current version in Python code.

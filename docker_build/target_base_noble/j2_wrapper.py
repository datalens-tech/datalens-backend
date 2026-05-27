#!/usr/bin/python3
"""Tiny Jinja2 CLI replacement for j2cli.

Renders TEMPLATE_FILE with environment variables as context and writes the
result to stdout. Covers the only j2 invocation pattern used by bi_base_mess
service scripts (etc/service/*/run, etc/my_init.d/*): `j2 file.j2 > output`.

Uses the apt-installed python3-jinja2 package so no pip/venv/pipx layer is
needed. j2cli is archived (June 2024) and breaks on Python 3.12 (pkg_resources
+ imp removals); jinjanator is the maintained replacement but isn't on
pypi.yandex-team.ru. This wrapper avoids both problems.
"""

import os
import sys

from jinja2 import (
    Environment,
    FileSystemLoader,
)


def main() -> None:
    if len(sys.argv) != 2:
        sys.stderr.write("usage: j2 TEMPLATE_FILE\n")
        sys.exit(2)
    path = sys.argv[1]
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(os.path.abspath(path))),
        keep_trailing_newline=True,
    )
    sys.stdout.write(env.get_template(os.path.basename(path)).render(os.environ))


if __name__ == "__main__":
    main()

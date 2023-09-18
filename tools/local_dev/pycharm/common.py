from __future__ import annotations

from os import path

HERE = path.abspath(path.dirname(__file__))

IDEA_DIR = path.abspath(path.join(HERE, "..", "..", "..", ".idea"))

PKGLIST_PATH = path.abspath(
    path.join(
        HERE,
        "..",
        "requirements",
        "all_local_packages.lst",
    )
)


def get_submodules():
    with open(PKGLIST_PATH) as fobj:
        data = list(fobj)
    data = [row.split("#", 1)[0].strip() for row in data]
    data = [item for item in data if item]
    return data


PKG_ROOT_PATH = "mirror"  # relative to home

SUBMODULE_DEFAULT_EXCLUDES = [
    ".idea",
    ".git",
    ".DS_Store",
    "build",
    "dist",
    ".eggs",
    ".pytest_cache",
    ".coverage",
    ".mypy_cache",
    ".tox",
    ".venv",
    "docker-data-volumes",
    "docker-compose/common/secrets/private",
]

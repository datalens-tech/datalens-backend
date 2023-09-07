from __future__ import annotations

import os

from library.python.testing import recipe
import logging

from datalens.backend.tools import dblib_common


# # sbr://1592020440
# PATH = 'oracle_instantclient_basiclite'
# # Original zip links are:
# #   libclntsh.so -> libclntsh.so.18.1
# #   libocci.so -> libocci.so.18.1
# # However, the resource got weirdly mangled
# LINKS = (
#     # symlink_path, symlink_destination_path
#     ('libclntsh.so.18.1', 'libclntsh.so'),
#     ('libocci.so', 'libocci.so.18.1'),
# )

# # sbr://1595620876
PATH = 'oracle_instantclient_lite_with_deps'
LINKS = ()


def start(argv):
    logger = logging.getLogger(__name__)
    path = PATH
    dblib_common.add_to_ldlp(name='oracle_libs', path=path, logger=logger)

    # Recover some symlinks which were present in the original archive but lost
    # in the sandbox resource:
    for link_path, link_target in LINKS:
        # `os.symlink`, like `ln -s`, somewhat imitates the effect of `cp`,
        # except the `src` is relative to the link itself.
        os.symlink(src=link_target, dst=os.path.join(path, link_path))

    recipe.set_env('NLS_LANG', '.AL32UTF8')
    logger.info('ready.')


def stop(argv):
    # TODO?: delete the PATH?
    pass


if __name__ == '__main__':
    recipe.declare_recipe(start, stop)

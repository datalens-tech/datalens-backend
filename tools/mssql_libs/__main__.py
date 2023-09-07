from __future__ import annotations

import os

from library.python.testing import recipe
import logging

from datalens.backend.tools import dblib_common


# # sbr://1591908839
# PATH = 'mssql_libs'


# # sbr://1595719948
PATH = 'mssql_libs_with_deps'


def start(argv):
    logger = logging.getLogger(__name__)
    path = PATH
    path = os.path.realpath(path)

    dblib_common.add_to_ldlp(name='mssql_libs', path=path, logger=logger)

    # The resulting lookup is `/etc/../…`.
    ini_path = '..' + path + '/' + 'odbcinst.ini'
    logger.debug('  ... ODBCINSTINI=%s', ini_path)
    recipe.set_env('ODBCINSTINI', ini_path)

    logger.info('ready.')


def stop(argv):
    # TODO?: delete the PATH?
    pass


if __name__ == '__main__':
    recipe.declare_recipe(start, stop)
